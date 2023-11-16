#lang racket/base

(require json
         racket/unsafe/ops)

(define N 5)

(when (< N 1)
  (error (format "N must be positive, but was ~a" N)))

(define N-1 (- N 1))
(define N-2 (- N 2))

(struct post (_id title tags) #:prefab)
(struct related-posts (_id tags related) #:prefab)

(define (hash->post h)
  (post (hash-ref h '_id)
        (hash-ref h 'title)
        (map string->symbol (hash-ref h 'tags))))

(define (post->hash p)
  (hasheq '_id (post-_id p)
          'title (post-title p)
          'tags (map symbol->string (post-tags p))))

(define (related-posts->hash r)
  (hasheq '_id (related-posts-_id r)
          'tags (map symbol->string (related-posts-tags r))
          'related (map post->hash (related-posts-related r))))

(define (make-tag-map posts)
  (let ([tag-map (make-hasheq)])
    (for* ([index (in-range (vector-length posts))]
           [tag (in-list (post-tags (vector-ref posts index)))])
      (hash-update! tag-map
                    tag
                    (λ (related-indices) (cons index related-indices))
                    list))
    tag-map))

(define (read-posts path)
  (let ([hashes (with-input-from-file path read-json)])
    (for/vector #:length (length hashes)
                ([h (in-list hashes)])
      (hash->post h))))

(define (write-posts posts path)
  (with-output-to-file
    path
    (λ () (write-json (for/list ([p (in-vector posts)])
                        (related-posts->hash p))))
    #:exists 'replace))

(define (tally tag-map post index posts-len)
  (let ([counts (make-bytes posts-len)])
    (for* ([tag (in-list (post-tags post))]
           [related-index (in-list (hash-ref tag-map tag))])
      (unsafe-bytes-set! counts
                         related-index
                         (add1 (bytes-ref counts related-index))))
    (unsafe-bytes-set! counts index 0) ;; remove self
    counts))

(define (top-n counts posts posts-len)
  (let ([min-count 0]
        [top-counts (make-bytes N)]
        [top-indices (make-vector N)])
    (for ([index (in-range posts-len)])
      (let ([count (bytes-ref counts index)])
        (when (> count min-count)
          (let loop ([rank N-2])
            (if (and (>= rank 0)
                     (> count (bytes-ref top-counts rank)))
              (loop (sub1 rank))
              (let ([rank (add1 rank)])
                (when (< rank N-1)
                  (for ([rank (in-inclusive-range N-2 rank -1)])
                    (unsafe-bytes-set! top-counts
                                       (add1 rank)
                                       (bytes-ref top-counts rank))
                    (vector-set! top-indices
                                 (add1 rank)
                                 (vector-ref top-indices rank))))
                (unsafe-bytes-set! top-counts rank count)
                (vector-set! top-indices rank index)
                (set! min-count
                      (bytes-ref top-counts N-1))))))))
    (for/list ([index (in-vector top-indices)])
      (vector-ref posts index))))

(define (process posts)
  (let ([posts-len (vector-length posts)]
        [tag-map (make-tag-map posts)])
    (for/vector #:length posts-len
                ([index (in-range posts-len)])
      (let* ([post (vector-ref posts index)]
             [counts (tally tag-map post index posts-len)])
        (related-posts (post-_id post)
                       (post-tags post)
                       (top-n counts posts posts-len))))))

(module+ main
  ;; `input` and `output` paths are relative to directory of this script
  ;; cli exec: $ racket related.rkt
  (define input "../posts.json")
  (define output "../related_posts_racket.json")
  (define posts (read-posts input))
  (collect-garbage)
  (define start-time (current-inexact-monotonic-milliseconds))
  (define related (process posts))
  (define end-time (current-inexact-monotonic-milliseconds))
  (write-posts related output)
  (printf "Processing time (w/o IO): ~ams~n"
          (real->decimal-string (- end-time start-time) 2)))
