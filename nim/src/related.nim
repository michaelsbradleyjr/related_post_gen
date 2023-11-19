import std/[hashes, monotimes, sugar, tables, times]
import pkg/[decimal, jsony, xxhash]

# questions re: performance in Azure VM/Docker
# --------------------------------------------
# ? is returning seq from findTopN faster than returning array
# ? is `collect(newSeqOfCap(posts.len)):` faster than `collect:`
# ? is `object` or `ref object` faster re: `type RelatedPosts`
# ? is it faster with or without inlining hints
# ? how to correctly round DecimalType to 2 places

# ^ unfortunately, the answers to some questions (and what combination of
# changes is overall fastest) can depend on choice of clang vs. gcc

const N: Positive = 5

type
  Post = ref object
    `"_id"`: string
    title: string
    tags : ref seq[string]

  RelatedPosts = object
    `"_id"`: string
    tags : ref seq[string]
    related: array[N, Post]

const
  input = "../posts.json"
  output = "../related_posts_nim.json"

func hash(x: string): Hash {.inline, used.} =
  cast[Hash](XXH3_64bits(x))

func genTagMap(posts: seq[Post]): Table[string, seq[int]] =
  result = initTable[string, seq[int]](100)
  for i, post in posts:
    for tag in post.tags[]:
      result.withValue(tag, val):
        val[].add i
      do:
        result[tag] = @[i]

proc readPosts(path: string): seq[Post] =
  path.readFile.fromJson(seq[Post])

proc writePosts(path: string, posts: seq[RelatedPosts]) =
  path.writeFile(posts.toJson)

{.push inline.}

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
        for j in countdown((N-2)*2, pos, 2):
          topN[j+2] = topN[j]
          topN[j+3] = topN[j+1]
      topN[pos] = count
      topN[pos+1] = i
      minCount = topN[(N-1)*2]
  for i in 0..<N:
    result[i] = posts[topN[i*2+1]]

func process(posts: seq[Post]): seq[RelatedPosts] =
  let tagMap = posts.genTagMap
  # collect(newSeqOfCap(posts.len)):
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
  output.writePosts(relatedPosts)
  echo "Processing time (w/o IO): ", timeMs, "ms"

when isMainModule:
  main()
