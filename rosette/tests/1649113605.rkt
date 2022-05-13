#lang rosette

(require "../util.rkt" "../sql.rkt" "../table.rkt"  "../evaluator.rkt" "../equal.rkt" "../cosette.rkt" "../denotation.rkt" "../syntax.rkt")

(define INDIV_SAMPLE_NYC (Table "INDIV_SAMPLE_NYC" (list "CMTE_ID" "TRANSACTION_AMT" "NAME" "STR_NAME") (gen-sym-schema 4 1)))


(define q1s (SELECT (VALS "INDIV_SAMPLE_NYC.CMTE_ID" "INDIV_SAMPLE_NYC.TRANSACTION_AMT" "INDIV_SAMPLE_NYC.NAME" "INDIV_SAMPLE_NYC.STR_NAME") FROM (NAMED INDIV_SAMPLE_NYC) WHERE (BINOP "INDIV_SAMPLE_NYC.CMTE_ID" = 1)))

(define q2s (SELECT (VALS "INDIV_SAMPLE_NYC.CMTE_ID" "INDIV_SAMPLE_NYC.TRANSACTION_AMT" "INDIV_SAMPLE_NYC.NAME" "INDIV_SAMPLE_NYC.STR_NAME") FROM (NAMED INDIV_SAMPLE_NYC) WHERE (BINOP "INDIV_SAMPLE_NYC.CMTE_ID" = 2)))


(cond
	[(eq? #f #t) println("LIKE regex does not match")]
	[(eq? #f #t) println("ORDER BY does not match")]
	[(eq? #t #t)
		(let* ([model (verify (same q1s q2s))]
			   [concrete-t1 (clean-ret-table (evaluate INDIV_SAMPLE_NYC model))])
			(println concrete-t1)
)])