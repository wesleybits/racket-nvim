#lang racket

(require binary-class
         racket/class)

(define ((between? x y) n)
  (<= x n y))

(define (unsigned->signed n size)
  (let ([max (inexact->exact (- (floor (/ (expt 2 (* size 8)) 2)) 1))])
    (if (> n max)
        (- n (* 2 (+ 1 max)))
        n)))

(define-binary-class msgpack%
  ([tag u1])
  #:dispatch
  (match tag
    [(? (between? #x00 #x7f)) positive-fixint%]
    [(? (between? #x80 #x8f)) fixmap%]
    [(? (between? #x90 #x9f)) fixarray%]
    [(? (between? #xa0 #xbf)) fixstring%]
    [#xc0 nil%]
    [#xc2 false%]
    [#xc3 true%]
    [#xc4 bin-8%]
    [#xc5 bin-16%]
    [#xc6 bin-32%]
    [#xc7 ext-8%]
    [#xc8 ext-16%]
    [#xc9 ext-32%]
    [#xca float-32%]
    [#xcb float-64%]
    [#xcc uint-8%]
    [#xcd uint-16%]
    [#xce uint-32%]
    [#xcf uint-64%]
    [#xd0 int-8%]
    [#xd1 int-16%]
    [#xd2 int-32%]
    [#xd3 int-64%]
    [#xd4 fixext-1%]
    [#xd5 fixext-2%]
    [#xd6 fixext-4%]
    [#xd7 fixext-8%]
    [#xd8 fixext-16%]
    [#xd9 str-8%]
    [#xda str-16%]
    [#xdb str-32%]
    [#xdc array-16%]
    [#xdd array-32%]
    [#xde map-16%]
    [#xdf map-32%]
    [(? (between? #xe0 #xff)) negative-fixint%]))

(define-binary-class nil% msgpack% ([val '()]))

(define-binary-class false% msgpack% ([val #false]))

(define-binary-class true% msgpack% ([val #true]))

(define-binary-class positive-fixint% msgpack%
  ([val tag]))

(define-binary-class negative-fixint% msgpack%
  ([val (unsigned->signed tag 1)]))

(define-binary-class uint-8% msgpack%
  ([val (integer-be 1)]))

(define-binary-class uint-16% msgpack%
  ([val (integer-be 2)]))

(define-binary-class uint-32% msgpack%
  ([val (integer-be 4)]))

(define-binary-class uint-64% msgpack% 
  ([val (integer-be 8)]))

(define-binary-class int-8% msgpack%
  ([val (signed integer-be 1)]))

(define-binary-class int-16% msgpack% 
  ([val (signed integer-be 2)]))

(define-binary-class int-32% msgpack% 
  ([val (signed integer-be 4)]))

(define-binary-class int-64% msgpack% 
  ([val (signed integer-be 8)]))

(define-binary-class float-32% msgpack%
  ([val float-be]))

(define-binary-class float-64% msgpack%
  ([val double-be]))

(define-binary-class fixstring% msgpack%
  ([val (bytestring (bitwise-and tag #b1111))]))

(define-binary-class str-8% msgpack%
  ([size (integer-be 1)]
   [val (bytestring size)]))

(define-binary-class str-16% msgpack%
  ([size (integer-be 2)]
   [val (bytestring size)]))

(define-binary-class str-32% msgpack% 
  ([size (integer-be 4)]
   [val (bytestring size)]))

(define-binary-class bin-8% str-8% ())
(define-binary-class bin-16% str-16% ())
(define-binary-class bin-32% str-32% ())

(define (msgpack-objects size)
  (binary (λ (in)
            (let loupe ([s size])
              (if (= s 0) '()
                (read-value msgpack% in))))
          (λ (out vals)
            (for ([v vals])
              (write-value msgpack% out v)))))

(define-binary-class fixarray% msgpack%
  ([contents (msgpack-objects (bitwise-and tag #b1111))]))

(define-binary-class array-16% msgpack% 
  ([size (integer-be 2)]
   [contents (msgpack-objects size)]))

(define-binary-class array-32% msgpack% 
  ([size (integer-be 4)]
   [contents (msgpack-objects size)]))

(define msgpack
  (binary (λ (in) (read-value msgpack% in))
          (λ (out v) (write-value msgpack% out v))))

(define-binary-class key-value-pair%
  ([key msgpack]
   [val msgpack]))

(define (msgpack-map size)
  (binary (λ (in)
            (let loupe ([s size])
              (if (= s 0) '()
                (cons (read-value key-value-pair% in)
                      (loupe (- s 1))))))
          (λ (out vals)
            (for ([v vals])
              (write-value msgpack% out v)))))

(define-binary-class fixmap% msgpack%
  ([contents (msgpack-map (bitwise-and tag #b1111))]))

(define-binary-class map-16% msgpack%
  ([size (integer-be 2)]
   [contents (msgpack-map size)]))

(define-binary-class map-32% msgpack% 
  ([size (integer-be 4)]
   [contents (msgpack-map size)]))

(define-binary-class fixext-1% msgpack% 
  ([type (integer-be 1)]
   [data (bytestring 1)]))

(define-binary-class fixext-2% msgpack%
  ([type (integer-be 1)]
   [data (bytestring 2)]))

(define-binary-class fixext-4% msgpack% 
  ([type (integer-be 1)]
   [data (bytestring 4)]))

(define-binary-class fixext-8% msgpack%
  ([type (integer-be 1)]
   [data (bytestring 8)]))

(define-binary-class fixext-16% msgpack% 
  ([type (integer-be 1)]
   [data (bytestring 16)]))

(define-binary-class ext-8% msgpack% 
  ([size (integer-be 1)]
   [type (integer-be 1)]
   [data (bytestring size)]))

(define-binary-class ext-16% msgpack% 
  ([size (integer-be 2)]
   [type (integer-be 1)]
   [data (bytestring size)]))

(define-binary-class ext-32% msgpack% 
  ([size (integer-be 4)]
   [type (integer-be 1)]
   [data (bytestring size)]))

(define ((is-any-of? . classes) obj)
  (ormap (λ (t) (is-a? obj t)) classes))

(struct ext (type data))

(define (unpack packed)
  (match packed
    [(? (is-any-of? positive-fixint%  negative-fixint%
                    nil%      false%   true%
                    bin-8%    bin-16%  bin-32% 
                    float-32% float-64%
                    uint-8%   uint-16% uint-32% uint-64%
                    int-8%    int-16%  int-32%  int-64%))
     (get-field val packed)]

    [(? (is-any-of? fixstring% str-8% str-16% str-32%))
     (bytes->string/utf-8 (get-field val packed))]

    [(? (is-any-of? fixarray% array-16% array-32%))
     (map unpack (get-field contents packed))]

    [(? (is-any-of? fixmap% map-16% map-32%))
     (local [(define keys (map (λ (o) (unpack (get-field key o)))
                               (get-field contents packed)))
             (define vals (map (λ (o) (unpack (get-field val o)))
                               (get-field contents packed)))]
       (make-immutable-hash
         (map cons keys vals)))]

    [(? (is-any-of? fixext-1% fixext-2% fixext-4% fixext-8% fixext-16% 
                    ext-8% ext-16% ext-32%))
     (ext (get-field type packed)
          (get-field data packed))]))

(define (read-msgpack [in (current-input-port)])
  (local [(define packed (read-value msgpack% in))]
    (unpack packed)))

(provide read-msgpack
         (struct-out ext))
