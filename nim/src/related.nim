import std/[hashes, monotimes, sugar, tables, times]
import pkg/[decimal, jsony, xxhash]

const N: Positive = 5

type
  Post = ref object
    `"_id"`: string
    title: string
    tags : ref seq[string]

  RelatedPosts = ref object
    `"_id"`: string
    tags : ref seq[string]
    related: array[N, Post]

const
  input = "../posts.json"
  output = "../related_posts_nim.json"

func hash(x: string): Hash {.inline.} =
  cast[Hash](XXH3_64bits(x))

proc readPosts(path: string): seq[Post] =
  path.readFile.fromJson(seq[Post])

proc writePosts(path: string, posts: seq[RelatedPosts]) =
  path.writeFile(posts.toJson)

{.push inline.}

func genTagMap(posts: seq[Post]): Table[string, seq[int]] =
  result = initTable[string, seq[int]](100)
  for i, post in posts:
    for tag in post.tags[]:
      result.withValue(tag, val):
        val[].add i
      do:
        result[tag] = @[i]

func countTaggedPost(
    posts: seq[Post],
    tagMap: Table[string, seq[int]],
    i: int): seq[uint8] =
  result = newSeq[uint8](posts.len)
  for tag in posts[i].tags[]:
    try:
      for relatedIDX in tagMap[tag]:
        inc result[relatedIDX]
    except KeyError as e:
      raise (ref Defect)(msg: e.msg)
  result[i] = 0 # remove self

func findTopN(
    posts: seq[Post],
    tagMap: Table[string, seq[int]],
    i: int): array[N, Post] =
  let taggedPostCount = countTaggedPost(posts, tagMap, i)
  var
    minCount = 0
    topN: array[N*2, int]
  # counts for related posts in even indices of topN
  # indices of related posts in odd indices of topN
  for i, count in taggedPostCount:
    let count = count.int
    if count > minCount:
      var pos = (N-2)*2
      while (pos >= 0) and (count > topN[pos]):
        dec(pos, 2)
      inc(pos, 2)
      if pos < (N-1)*2:
        copyMem(addr topN[pos+2], addr topN[pos], sizeof(int)*((N*2-3)-pos+1))
      topN[pos] = count
      topN[pos+1] = i
      minCount = topN[(N-1)*2]
  for i in 0..<N:
    result[i] = posts[topN[i*2+1]]

func process(posts: seq[Post]): seq[RelatedPosts] =
  let tagMap = posts.genTagMap
  collect:
    for i, post in posts:
      RelatedPosts(
        `"_id"`: post.`"_id"`,
        tags: post.tags,
        related: posts.findTopN(tagMap, i))

{.pop.}

proc main() =
  let
    posts = input.readPosts
    t0 = getMonotime()
    relatedPosts = posts.process
    t1 = getMonotime()
    timeNs: int64 = (t1 - t0).inNanoseconds
    timeMs: DecimalType = newDecimal(timeNs) / newDecimal(1_000_000)
    timeMsS = $timeMs
  setPrec(4)
  let time = newDecimal(timeMsS)
  output.writePosts(relatedPosts)
  echo "Processing time (w/o IO): ", time, "ms"

when isMainModule:
  main()
