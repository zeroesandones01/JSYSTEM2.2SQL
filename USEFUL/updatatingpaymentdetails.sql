--QUERY FOR UPDATING PAYMENT DETAILS

--MOST UPDATING OF PAYMENT DETAILS ARE IN THESE TABLES
--(rf_pay_header, rf_pay_detail, rf_payments, rf_crb_header, rf_crb_detail)

--determine what details might be updated in these tables and find if each table has the corresponding column
--find the exact row or to be updated in each of the table

--EXAMPLE OF UPDATING CHECK NUMBER OF PARTICULAR PAYMENT
--the tables that will be updated are rf_pay_detail and rf_payments since they are only the tables that have check_no as column

BEGIN
select check_no, * from rf_pay_detail where client_seqno = 'GA0110019208' and entity_id = '0000000127'-- and check_no = '2847.45';
--update rf_pay_detail set check_no = '9072240' where client_seqno = 'GA0110019208' and entity_id = '0000000127' and check_no = '2847.45';

select check_no, pay_rec_id, get_employee_name(created_by), * from rf_payments where client_seqno = 'GA0110019208';
--UPDATE rf_payments set check_no = '9072240' where client_seqno = 'GA0110019208';

COMMIT

SELECT * FROM rf_payments where check_no = '9072240';
select * from rf_pay_detail where check_no = '9072240';

select * from rf_check_history limit 1;


select * from cs_dp_chk_detail where pay_rec_id::INT = 790586;
