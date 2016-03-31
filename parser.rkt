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

(define flags
  (hash 'fixmap    #x80
        'fixarray  #x90
        'fixstring #xa0
        'nil       #xc0
        'false     #xc2
        'true      #xc3
        'bin-8     #xc4
        'bin-16    #xc5
        'bin-32    #xc6
        'ext-8     #xc7
        'ext-16    #xc8
        'ext-32    #xc9
        'float-32  #xca
        'float-64  #xcb
        'uint-8    #xcc
        'uint-16   #xcd
        'uint-32   #xce
        'uint-64   #xcf
        'int-8     #xd0
        'int-16    #xd1
        'int-32    #xd2
        'int-64    #xd3
        'fixext-1  #xd4
        'fixext-2  #xd5
        'fixext-4  #xd6
        'fixext-8  #xd7
        'fixext-16 #xd8
        'str-8     #xd9
        'str-16    #xda
        'str-32    #xdb
        'array-16  #xdc
        'array-32  #xdd
        'map-16    #xde
        'map-32    #xdf))

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

(define (packable-list? l)
  (and (list? l)
       (andmap packable? l)))

(define (packable-hash? h)
  (and (hash? h)
       (andmap (match-lambda
                 [(cons (? packable?) (? packable?)) #true]
                 [_ #false])
               (hash->list h))))

(define (packable? rkt)
  (ormap (λ (p) (p rkt))
         (list (λ (n) (and (number? n) (integer? n)))
               (λ (n) (and (number? n) (inexact? n)))
               (λ (n) (and (number? n) (rational? n)))
               string?
               bytes?
               boolean?
               ext?
               null?
               packable-list?
               packable-hash?)))

(define (pack-integer n)
  (match n
    [(? (between? 0 127))
     (local [(define packed (new positive-fixint%))]
       (set-field! tag packed n)
       packed)]

    [(? (between? -32 -1))
     (local [(define packed (new negative-fixint%))]
       (set-field! tag packed n)
       packed)]

    [(or 127
         (? (between? -33 -128)))
     (local [(define packed (new int-8%))]
       (set-field! tag packed (hash-ref flags 'int-8))
       (set-field! val packed n)
       packed)]

    [(or (? (between?  128  32767))
         (? (between? -129 -32168)))
     (local [(define packed (new int-16%))]
       (set-field! tag packed (hash-ref flags 'int-16))
       (set-field! val packed n)
       packed)]

    [(or (? (between?  32768  2147483647))
         (? (between? -32768 -2147483648)))
     (local [(define packed (new int-32%))]
       (set-field! tag packed (hash-ref flags 'int-32))
       (set-field! val packed n)
       packed)]

    [_
     (local [(define packed (new int-64%))]
       (set-field! tag packed (hash-ref flags 'int-64))
       (set-field! val packed n)
       packed)]))

(define (pack-floating n)
  (local [(define packed (new float-64%))]
    (set-field! tag packed (hash-ref flags 'float-64))
    (set-field! val packed n)
    packed))

(define (pack-string str)
  (define bytestr (string->bytes/utf-8 str))
  (match (bytes-length bytestr)
    [(? (between? 0 31))
     (local [(define packed (new fixstring%))
             (define flag (bitwise-ior (hash-ref flags 'fixstring)
                                      (bytes-length bytestr)))]
       (set-field! tag packed flag)
       (set-field! val packed bytestr)
       packed)]
    [(? (between? 32 255))
     (local [(define packed (new str-8%))]
       (set-field! tag packed (hash-ref flags 'str-8))
       (set-field! size packed (bytes-length bytestr))
       (set-field! val packed bytestr)
       packed)]
    [(? (between? 256 65535))
     (local [(define packed (new str-16%))]
       (set-field! tag packed (hash-ref flags 'str-16))
       (set-field! size packed (bytes-length bytestr))
       (set-field! val packed bytestr)
       packed)]
    [(? (between? 65536 4294967295))
     (local [(define packed (new str-32%))]
       (set-field! tag packed (hash-ref flags 'str-32))
       (set-field! size packed (bytes-length bytestr))
       (set-field! val packed bytestr)
       packed)]
    [_ (error (~a "string is too long: " (bytes-length bytestr)))]))

(define (pack-ext x)
  (match-define (ext type data) x)
  (match (bytes-length data)
    [1 (local [(define packed (new fixext-1%))]
         (set-field! tag packed (hash-ref flags 'fixext-1))
         (set-field! type packed type)
         (set-field! data packed data)
         packed)]
    [2 (local [(define packed (new fixext-2%))]
         (set-field! tag packed (hash-ref flags 'fixext-2))
         (set-field! type packed type)
         (set-field! data packed data)
         packed)]
    [4 (local [(define packed (new fixext-4%))]
         (set-field! tag packed (hash-ref flags 'fixext-4))
         (set-field! type packed type)
         (set-field! data packed data)
         packed)]
    [8 (local [(define packed (new fixext-8%))]
         (set-field! tag packed (hash-ref flags 'fixext-8%))
         (set-field! type packed type)
         (set-field! data packed data)
         packed)]
    [16 (local [(define packed (new fixext-16%))]
          (set-field! tag packed (hash-ref flags 'fixext-16))
          (set-field! type packed type)
          (set-field! data packed data)
          packed)]
    [(? (between? 0 255))
     (local [(define packed (new ext-8%))]
       (set-field! tag packed (hash-ref flags 'ext-8))
       (set-field! size packed (bytes-length data))
       (set-field! type packed type)
       (set-field! data packed data)
       packed)]
    [(? (between? 256 65535))
     (local [(define packed (new ext-16%))]
       (set-field! tag packed (hash-ref flags 'ext-16))
       (set-field! size packed (bytes-length data))
       (set-field! type packed type)
       (set-field! data packed data)
       packed)]
    [(? (between? 65536 4294967295))
     (local [(define packed (new ext-32%))]
       (set-field! tag packed (hash-ref flags 'ext-32))
       (set-field! size packed (bytes-length data))
       (set-field! type packed type)
       (set-field! data packed data)
       packed)]))

(define (pack-bytes bytestr)
  (match (bytes-length bytestr)
    [(? (between? 16 255))
     (local [(define packed (new bin-8%))]
       (set-field! tag packed (hash-ref flags 'bin-8))
       (set-field! size packed (bytes-length bytestr))
       (set-field! val packed bytestr)
       packed)]
    [(? (between? 256 65535))
     (local [(define packed (new bin-16%))]
       (set-field! tag packed (hash-ref flags 'bin-16))
       (set-field! size packed (bytes-length bytestr))
       (set-field! val packed bytestr)
       packed)]
    [(? (between? 65536 4294967295))
     (local [(define packed (new bin-32%))]
       (set-field! tag packed (hash-ref flags 'bin-32))
       (set-field! size packed (bytes-length bytestr))
       (set-field! val packed bytestr)
       packed)]
    [_ (error (~a "string is too long: " (bytes-length bytestr)))]))

(define (pack-list lst)
  (match (length lst)
    [(? (between? 0 15))
     (local [(define packed (new fixarray%))
             (define flag (bitwise-ior (hash-ref flags 'fixarray)
                                      (length lst)))]
       (set-field! tag packed flag)
       (set-field! contents packed (map pack lst))
       packed)]
    [(? (between? 16 65535))
     (local [(define packed (new array-16%))]
       (set-field! tag packed (hash-ref flags 'array-16))
       (set-field! size packed (length lst))
       (set-field! contents packed (map pack lst))
       packed)]
    [(? (between? 65536 4294967295))
     (local [(define packed (new array-32%))]
       (set-field! tag packed (hash-ref flags 'array-32))
       (set-field! size packed (length lst))
       (set-field! contents packed (map pack lst))
       packed)]))

(define (pack-hash hsh)
  (define/match (pack-pair p)
    [((cons (app pack k) (app pack v)))
     (local [(define packed (new key-value-pair%))]
       (set-field! key packed k)
       (set-field! val packed v)
       packed)])
  (match (length (hash-keys hsh))
    [(? (between? 0 15))
     (local [(define packed (new fixmap%))
             (define flag (bitwise-ior (hash-ref flags 'fixmap)
                                      (length (hash-keys hsh))))]
       (set-field! tag packed flag)
       (set-field! contents packed (map pack-pair (hash->list hsh)))
       packed)]
    [(? (between? 16 65535))
     (local [(define packed (new map-16%))]
       (set-field! tag packed (hash-ref flags 'map-16))
       (set-field! size packed (length (hash-keys hsh)))
       (set-field! contents packed (map pack-pair (hash->list hsh)))
       packed)]
    [(? (between? 65536 4294967295))
     (local [(define packed (new map-32%))]
       (set-field! tag packed (hash-ref flags 'map-32))
       (set-field! size packed (length (hash-keys hsh)))
       (set-field! contents packed (map pack-pair (hash->list hsh)))
       packed)]))

(define (pack rkt)
  (match rkt
    ;; trivial cases (nil, true, false)
    [(? null?)
     (local [(define packed (new nil%))]
       (set-field! tag packed (hash-ref flags 'nil))
       packed)]
    [#false
     (local [(define packed (new false%))]
       (set-field! tag packed (hash-ref flags 'false))
       packed)]
    [#true
     (local [(define packed (new true%))]
       (set-field! tag packed (hash-ref flags 'true))
       packed)]

    ;; integers
    [(and (? number?) (? integer?))
     (pack-integer rkt)]

    ;; non-integers (and non-complex) numbers
    [(and (? number?) (? inexact?))
     (pack-floating rkt)]
    [(and (? number?) (? rational?))
     (pack-floating (exact->inexact rkt))]

    ;; strings and byte arrays
    [(? string?)
     (pack-string rkt)]
    [(? bytes?)
     (pack-bytes rkt)]

    ;; the extended type
    [(? ext?)
     (pack-ext rkt)]

    ;; structs (lists and hashes)
    [(? packable-list?)
     (pack-list rkt)]
    [(? packable-hash?)
     (pack-hash rkt)]

    ;; if it's not packable
    [_ (error (~a "not a packable datum: " rkt))]))

(define (write-msgpack v [out (current-output-port)])
  (local [(define msg (pack v))]
    (write-value msgpack% out msg)))

(provide read-msgpack
         write-msgpack
         packable?
         (struct-out ext))
