#lang racket

(require rackunit
         "../msgpack.rkt")

;; {"compact": true, "schema":0}
(define example1
  (bytes #x82 #xa7 #x63 #x6f
         #x6d #x70 #x61 #x63
         #x74 #xc3 #xa6 #x73
         #x63 #x68 #x65 #x6d
         #x61 #x00))

(define answer1
  #hash(("compact" . #t)
        ("schema" . 0)))

(define example2
  (bytes #x82 #xa7 #x63 #x6f
         #x6d #x70 #x61 #x63
         #x74 #xc3 #xa6 #x73
         #x63 #x68 #x65 #x6d
         #x61 #xcd #x30 #x39))

(define answer2
  #hash(("compact" . #t)
        ("schema" . 12345)))

(define example3
  (bytes #x82 #xa7 #x63 #x6f
         #x6d #x70 #x61 #x63
         #x74 #xc3 #xa6 #x73
         #x63 #x68 #x65 #x6d
         #x61 #xf4))

(define answer3
  #hash(("compact" . #t)
        ("schema" . -12)))

(define example4
  (bytes #x82 #xa7 #x63 #x6f
         #x6d #x70 #x61 #x63
         #x74 #xc3 #xa6 #x73
         #x63 #x68 #x65 #x6d
         #x61 #xd2 #xff #xed
         #x29 #x79))

(define answer4
  #hash(("compact" . #t)
        ("schema" . -1234567)))

(define example5
  (bytes #x82 #xa7 #x63 #x6f
         #x6d #x70 #x61 #x63
         #x74 #xc3 #xa6 #x73
         #x63 #x68 #x65 #x6d
         #x61 #x94 #xf4 #x22
         #x38 #x07))

(define answer5
  #hash(("compact" . #t)
        ("schema" . (-12 34 56 7))))

(define example6
  (bytes #x83 #xa7 #x63 #x6f
         #x6d #x70 #x61 #x63
         #x74 #xc2 #xa6 #x73
         #x63 #x68 #x65 #x6d
         #x61 #x94 #xf4 #x22
         #x38 #x07 #xb2 #x73
         #x69 #x6c #x6c #x79
         #x20 #x6c #x69 #x74
         #x74 #x6c #x65 #x20
         #x67 #x69 #x72 #x6c
         #x73 #xc0))

(define answer6
  #hash(("compact" . #f)
        ("schema" . (-12 34 56 7))
        ("silly little girls" . ())))

(define example7
  (bytes #xde #x00 #x14 #xa1
         #x31 #x01 #xa1 #x32
         #x02 #xa1 #x33 #x03
         #xa1 #x34 #x04 #xa1
         #x35 #x05 #xa1 #x36
         #x06 #xa1 #x37 #x07
         #xa1 #x38 #x08 #xa1
         #x39 #x09 #xa2 #x31
         #x30 #x0a #xa2 #x31
         #x31 #x0b #xa2 #x31
         #x32 #x0c #xa2 #x31
         #x33 #x0d #xa2 #x31
         #x34 #x0e #xa2 #x31
         #x35 #x0f #xa2 #x31
         #x36 #x10 #xa2 #x31
         #x37 #x11 #xa2 #x31
         #x38 #x12 #xa2 #x31
         #x39 #x13 #xa2 #x32
         #x30 #x14))

(define answer7
  (for/hash ([i (in-range 1 21)])
    (values (number->string i) i)))

(define example8
  (bytes #xdc #x00 #x14 #x01
         #x02 #x03 #x04 #x05
         #x06 #x07 #x08 #x09
         #x0a #x0b #x0c #x0d
         #x0e #x0f #x10 #x11
         #x12 #x13 #x14))

(define answer8
  (sequence->list (in-range 1 21)))

(define (test-packing example answer ordinal)
  (displayln (~a "Test set " ordinal ": " answer))
  (local [(define example-result
            (with-input-from-bytes example
              (λ () (read-msgpack))))

          (define example-output
            (with-output-to-bytes
              (λ () (write-msgpack example-result))))

          (define example-re-read
            (with-input-from-bytes example-output
              (λ () (read-msgpack))))]
    (check-pred packable? example-output "Read struct is packable")
    (check-equal? example-result answer "Parsed struct is what's expected")
    (check-equal? example-result example-re-read "Output bin parses to the same struct")))

(test-packing example1 answer1 1)
(test-packing example2 answer2 2)
(test-packing example3 answer3 3)
(test-packing example4 answer4 4)
(test-packing example5 answer5 5)
(test-packing example6 answer6 6)
(test-packing example7 answer7 7)
(test-packing example8 answer8 8)
