#lang rosette

(require "../cosette.rkt" "../util.rkt" "../table.rkt" 
         "../sql.rkt" "../evaluator.rkt" "../equal.rkt")

(define t1 (Table "t1" (list "id" "val") (gen-sym-schema 2 3)))

(define-symbolic p1 (~> integer? integer? boolean?)) 

(define t2c (Table "t1" (list "id") (list (cons (list 0) 0))))

; SELECT * AS u FROM users WHERE id = 1

(define q1
  (SELECT (VALS "t1.id")
   FROM   (NAMED t1)
   WHERE  (BINOP "t1.id" = "t1.val"))) 

;(define q2 (NAMED t2))
(define q2
  (SELECT (VALS "t1.id")
   FROM   (NAMED t1)
   WHERE  (NARY-OP p1 "t1.id" "t1.val"))) 

; expect model
(cosette-solve q1 q2 (list t1 p1))

; expect unsat
;(time (verify (same q2 q2)))

;(denote-and-run q2)

;; (define nop (F-NARY-OP > "t1.id1" "t1.id")) 

;; (define (denote-nop f)                  
;;   `(apply 
;;       ,(filter-nary-op-f f)
;;       ,(let ([val (append '(list) (map (lambda (x)
;;                `(+ ,(string-length (val-column-ref-column-name x)) 1))
;;             (filter-nary-op-args f)))])
;;         val)))
;
;(denote-nop nop)
