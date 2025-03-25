--DEPOSITS MODULE

--DETERMINE THE DEPOSIT NUMBER AND COMPANY OF THE DATA TO BE EDITED

--query the data in the cs_dp_header 
select * from cs_dp_header where dep_no = '00020517' and co_id = '01';

--UPDATE dep_date, cash_date, remarks column based on the DCRF
BEGIN
--update cs_dp_header set dep_date = '2025-01-14 00:00:00', cash_date = '2025-01-10 00:00:00', remarks = 'TO RECORD DEPOSIT FOR 01/14/2025 FOR COLLECTION DATE 01/10/2025' where dep_no = '00020517' and co_id = '01';

COMMIT

select * from cs_dp_chk_detail where dep_no = '00020517';

select * from rf_check_history where pay_rec_id::INT = 790586;

select * from rf_jv_header where jv_no = '25010021' AND co_id = '01'