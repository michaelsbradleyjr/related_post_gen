import std/[algorithm, cpuinfo, hashes, math, monotimes, tables, times]
import pkg/[jsony, taskpools, xxhash]

# import nimsimd/runtimecheck
import nimsimd/sse3
# import nimsimd/avx2

# let cpuHasSse3 = checkInstructionSets({SSE3})
# echo ""
# echo "cpuHasSse3: " & $cpuHasSse3
# let cpuHasAvx2 = checkInstructionSets({AVX2})
# echo "cpuHasAvx2: " & $cpuHasAvx2
# echo ""

# Facilities to be used
# ---------------------
#
# SSE2
# ---------------------
# mm_cmpgt_epi8
# mm_set1_epi8
#
# AVX2
# ---------------------
# mm256_cmpgt_epi8
# mm256_set1_epi8

const
  N: Positive = 5
  VecSize = 16
  # VecSize = 32

static:
  assert VecSize.isPowerOfTwo

type
  Post = object
    `"_id"`: string
    title: string
    tags : seq[Tag]

  PostIndex = int

  PostOut = object
    `"_id"`: string
    tags : ptr seq[Tag]
    related: TopN

  RelCount = int8

  RelMeta = tuple[count: RelCount, index: PostIndex]

  Tag = string

  TagMap = Table[Tag, seq[PostIndex]]

  TaskGroup = tuple[first, last: PostIndex]

  TaskGroups = object
    groups: seq[TaskGroup]
    size: int

  TopN = array[N, ptr Post]

  CountVector = object
    counts {.align(VecSize).}: array[VecSize, RelCount]

  Counts = object
    vectors: seq[CountVector]

const
  falsy = block:
    var falsy: array[VecSize, RelCount]
    falsy
  input = "../posts.json"
  output = "../related_posts_nim_con.json"

func hash(x: Tag): Hash {.inline, used.} =
  cast[Hash](XXH3_64bits(x))

func len(v: CountVector): int =
  v.counts.len

func size(c: Counts): int =
  c.vectors.len * VecSize

# func `{}` ... for getting a whole vector in a Counts instance
# func `{}=` ... for setting a whole vector (all items) in a Counts instance to a scalar
# func `[]=` ... for setting an item within a vector in a Counts instance using offset math

func `[]`(t: TagMap, key: Tag): lent seq[PostIndex] =
  tables.`[]`(t.addr[], key)

func `[]`(groups: TaskGroups, i: int): TaskGroup =
  groups.groups[i]

proc dumpHook(s: var string, v: ptr) {.inline, used.} =
  if v == nil:
    s.add("null")
  else:
    s.dumpHook(v[])

func init(T: typedesc[TagMap], posts: seq[Post]): T =
  result = initTable[Tag, seq[PostIndex]](100)
  for i, post in posts:
    for tag in post.tags:
      result.withValue(tag, val):
        val[].add(i)
      do:
        result[tag] = @[i]

func init(T: typedesc[TaskGroups], taskCount, groupCount: int): T =
  var groups = newSeqOfCap[TaskGroup](groupCount)
  let width = taskCount div groupCount
  for i in 0..<groupCount:
    groups.add((first: width * i, last: width * (i + 1) - 1))
    if i == groupCount - 1:
      groups[i].last = groups[i].last + taskCount mod groupCount
  T(groups: groups, size: groups.len)

proc readPosts(path: string): seq[Post] =
  path.readFile.fromJson(seq[Post])

proc writePosts(path: string, posts: seq[PostOut]) =
  path.writeFile(posts.toJson)

{.push inline.}

proc tally(
    counts: var seq[RelCount],
    posts: ptr seq[Post],
    tagMap: ptr TagMap,
    index: PostIndex) =
  for tag in posts[index].tags:
    try:
      for relIndex in tagMap[][tag]:
        inc counts[relIndex]
    except KeyError as e:
      raise (ref Defect)(msg: e.msg)
  counts[index] = 0 # remove self

proc topN(
    counts: var seq[RelCount],
    posts: ptr seq[Post],
    metas: var array[N, RelMeta],
    related: var TopN) =
  let countsLen = counts.len
  var
    index = 0
    minCount = mm_set1_epi8(RelCount(0))
    # minCount = mm256_set1_epi8(RelCount(0))
  while index + VecSize - 1 < countsLen:
    var cmpCounts {.align(VecSize).}: array[VecSize, RelCount]
    for i in 0..<VecSize:
      cmpCounts[i] = counts[index + i]
    let
      cmpResult = mm_cmpgt_epi8(cast[M128i](cmpCounts), minCount)
      # cmpResult = mm256_cmpgt_epi8(cast[M256i](cmpCounts), minCount)
      canSkip = cast[array[VecSize, RelCount]](cmpResult) == falsy
    if canSkip:
      for i in 0..<VecSize:
        counts[index + i] = 0
      inc(index, VecSize)
      continue
    let nextVecIndex = index + VecSize
    while index < nextVecIndex:
      let
        count = counts[index]
        rank = N - 1
      if count > metas[rank].count:
        metas[rank].count = count
        metas[rank].index = index
        when N >= 2:
          for rank in countdown(N - 2, 0):
            if count > metas[rank].count:
              swap(metas[rank + 1], metas[rank])
        minCount = mm_set1_epi8(metas[N - 1].count)
        # minCount = mm256_set1_epi8(metas[N - 1].count)
      counts[index] = 0
      inc index
  for rank in 0..<N:
    related[rank] = addr posts[metas[rank].index]
    metas[rank].count = 0
    metas[rank].index = 0

proc process(
    group: TaskGroup,
    posts: ptr seq[Post],
    tagMap: ptr TagMap,
    postsOut: ptr seq[PostOut]) =
  let countsLen = ((posts[].len + VecSize - 1) div VecSize) * VecSize
  var counts = newSeq[RelCount](countsLen)
  var metas: array[N, RelMeta]
  for index in (group.first)..(group.last):
    counts.tally(posts, tagMap, index)
    postsOut[index].`"_id"` = posts[index].`"_id"`
    postsOut[index].tags = addr posts[index].tags
    counts.topN(posts, metas, postsOut[index].related)

{.pop.}

proc main() =
  let
    posts = input.readPosts
    t0 = getMonotime()
    tagMap = TagMap.init(posts)
    threadCount = countProcessors()
    groups = TaskGroups.init(posts.len, threadCount)
  var
    pool = Taskpool.new(threadCount)
    postsOut = newSeq[PostOut](posts.len)
  for i in 0..<groups.size:
    pool.spawn groups[i].process(addr posts, addr tagMap, addr postsOut)
  pool.syncAll
  let time = (getMonotime() - t0).inMicroseconds / 1000
  pool.shutdown
  output.writePosts(postsOut)
  echo "Processing time (w/o IO): ", time, "ms"

when isMainModule:
  main()
