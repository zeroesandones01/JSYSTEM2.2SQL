
BEGIN
SELECT bank_id from rf_payments where pay_rec_id = 781839;
select * from mf_bank where bank_id = '20';

--update rf_payments set bank_id = '20' where pay_rec_id = 781839

COMMIT


select * from mf_bank where TRIM(bank_id) = '51';

SELECT * FROM rf_entity where entity_id = '0308496748';
