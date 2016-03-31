#lang racket

(require rackunit
         "../parser.rkt")

;; {"compact": true, "schema":0}
(define example
  (bytes #x82 #xa7 #x63 #x6f
         #x6d #x70 #x61 #x63 
         #x74 #xc3 #xa6 #x73 
         #x63 #x68 #x65 #x6d 
         #x61 #x00))

(define example-result
  (with-input-from-bytes example
    (λ () (read-msgpack))))

(define example-output
  (with-output-to-bytes
    (λ () (write-msgpack example-result))))

(define example-re-read
  (with-input-from-bytes example-output
    (λ () (read-msgpack))))

(check-pred packable? example-output
            "Read structure is packable")
(check-equal? example example-output
              "Serialized structure very closely matches the original binary")
(check-equal? example-result example-re-read
              "Reading own generated binary produces the same structure")

