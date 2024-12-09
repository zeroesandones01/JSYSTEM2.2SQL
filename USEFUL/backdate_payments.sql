--QUERY FOR BACKDATING OF PAYMENTS

select * from mf_office_branch;
select * from rf_pay_header;
select * from rf_pay_detail;
select * from rf_payments;
select * from rf_crb_header;


select sum(total_amt_paid) from rf_pay_header where branch_id = '11' AND booking_date::DATE = CURRENT_DATE and co_id = '02';

select * from rf_pay_header where branch_id = '11' AND booking_date::DATE = CURRENT_DATE and co_id = '02';


SELECT get_employee_name(created_by), * FROM rf_pay_header where branch_id = '11' AND booking_date::DATE = '2024-12-08' and co_id = '01' and created_by = '901169' AND status_id = 'A';
--update rf_pay_header set trans_date = trans_date - '1 day'::INTERVAL, booking_date = booking_date - '1 day'::INTERVAL,date_created = date_created - '1 day'::INTERVAL where branch_id = '11' AND booking_date::DATE = CURRENT_DATE and co_id = '01';

select trans_date - '1 year'::INTERVAL, *  FROM rf_pay_header where branch_id = '11' AND booking_date::DATE = CURRENT_DATE and co_id = '01';
select * from rf_pay_header where branch_id = '11' AND booking_date::DATE = CURRENT_DATE and co_id = '01';
select pay_rec_id, * from rf_payments where branch_id = '11' and actual_date::DATE = CURRENT_DATE and co_id = '01' AND status_id = 'A';
--update rf_payments actual_date = actual_date - '1 day'::INTERVAL, trans_date = trans_date - '1 day'::interval, or_date = or_date - '1 day'::INTERVAL, 
post_date = post_date - '1 day'::INTERVAL,  
where branch_id = '11' and actual_date::DATE = CURRENT_DATE and co_id = '01' AND status_id = 'A';

select * from rf_crb_header where issued_date::DATE = CURRENT_DATE and co_id = '01';


--SKIP UPPER PART OF QUERY
BEGIN

update rf_pay_header set trans_date = trans_date - '1 day'::INTERVAL, booking_date = booking_date - '1 day'::INTERVAL,date_created = date_created - '1 day'::INTERVAL 
where branch_id = '11' AND booking_date::DATE = CURRENT_DATE and co_id = '02';


update rf_payments SET actual_date = actual_date - '1 day'::INTERVAL, trans_date = trans_date - '1 day'::interval, or_date = or_date - '1 day'::INTERVAL, 
post_date = post_date - '1 day'::INTERVAL  
where branch_id = '11' and actual_date::DATE = CURRENT_DATE and co_id = '02' AND status_id = 'A';

WITH payments AS (
	SELECT pay_rec_id from rf_payments where branch_id = '11' and actual_date::DATE = '2024-12-08' and co_id = '02' AND status_id = 'A'
)
update rf_crb_header set issued_date = issued_date - '1 day'::INTERVAL
from payments
where rf_crb_header.issued_date::DATE = CURRENT_DATE and rf_crb_header.co_id = '02' and rf_crb_header.pay_rec_id::INT = payments.pay_rec_id and rf_crb_header.status_id = 'A';


COMMIT