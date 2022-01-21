#lang rosette 
 
(require "../cosette.rkt" "../sql.rkt" "../evaluator.rkt" "../syntax.rkt") 
 
(provide ros-instance)
 
(current-bitwidth #f)
 
(define-symbolic div_ (~> integer? integer? integer?))
 
(define months-info (table-info "months" (list "mid" "month")))
 
(define weekdays-info (table-info "weekdays" (list "did" "day_of_week")))
 
(define carriers-info (table-info "carriers" (list "cid" "name")))
 
(define flights-info (table-info "flights" (list "fid" "origin_city" "dest_city" "actual_time" )))
 

(define (q1 tables) 
  (SELECT-DISTINCT (VALS "f.origin_city" "f.dest_city" "f.actual_time") 
  FROM (JOIN (AS (NAMED (list-ref tables 3)) ["f"]) (AS (SELECT (VALS "a.origin_city" (VAL-UNOP aggr-max (val-column-ref "a.actual_time"))) 
 FROM (AS (NAMED (list-ref tables 3)) ["a"]) 
 WHERE (TRUE) GROUP-BY (list "a.origin_city") 
 HAVING (TRUE)) ["x" (list "origin_city" "max_time")])) 
  WHERE (AND (BINOP "f.origin_city" = "x.origin_city") (BINOP "f.actual_time" = "x.max_time"))))

(define (q2 tables) 
  (SELECT (VALS "f1.origin_city" "f1.dest_city" "f1.actual_time") 
 FROM (JOIN (AS (NAMED (list-ref tables 3)) ["f1"]) (AS (NAMED (list-ref tables 3)) ["f2"])) 
 WHERE (BINOP "f1.origin_city" = "f2.origin_city") GROUP-BY (list "f1.origin_city" "f1.dest_city" "f1.actual_time") 
 HAVING (BINOP "f1.actual_time" = (VAL-UNOP aggr-max (val-column-ref "f2.actual_time")))))


(define ros-instance (list q1 q2 (list months-info weekdays-info carriers-info flights-info))) 
