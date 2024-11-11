
SELECT * FROM rf_itsreal_bir_soa where entity_id = '8155055289';

SELECT *
FROM rf_payments
where client_seqno = '030110513008'
and or_no = '003765B';

select * from mf_pay_particular limit 1;

SELECT * FROM view_or_receipt_itsreal('030110513008', '003765B');
SELECT * FROM view_or_receipt_breakdown_v3('030110513008', '003765B') ORDER BY c_particulars;
SELECT * FROM view_or_receipt_breakdown_v3($P{client_seqno}, $P{or_no}) ORDER BY c_particulars;
select * from view_or_receipt_payment_mode('030110513008', '003765B');
SELECT * FROM view_or_receipt_payment_mode('030050721001', '011181');
SELECT * FROM view_or_receipt_itsreal('030050721001', '011181');


SELECT * FROM view_or_receipt_breakdown_v3('030110513008', '003765B') ORDER BY c_particulars;


select * from rf_pay_header where client_seqno = '030110513008';
select * from rf_pay_detail where receipt_no = '003765B';
select remarks, * from rf_payments where client_seqno = '100191118035';

SELECT array_agg(partdesc) FROM mf_pay_particular WHERE pay_part_id IN (SELECT pay_part_id FROM rf_payments WHERE client_seqno = '030110513008' AND or_no = '003765B')

SELECT * FROM rf_payments a where a.client_seqno = '030110513008' and exists 
(SELECT * FROM rf_itsreal_bir_soa where TRIM(entity_id) = TRIM(a.entity_id) and TRIM(proj_id) = TRIM(a.proj_id) and TRIM(pbl_id) = TRIM(a.pbl_id) 
and seq_no = a.seq_no and TRIM(status_id) = 'A');

SELECT * FROM mf_pay_particular where partdesc ~*'SUBDIVISION';

SELECT * FROM rf_payments where pay_rec_id::INT = 787699;

select amount, * from rf_pay_detail where receipt_no ~*'031103B';
select total_amt_paid, * from rf_pay_header where client_seqno = '0124110500054';

