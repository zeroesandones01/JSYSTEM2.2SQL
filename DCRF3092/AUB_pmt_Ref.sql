
WITH aub_ref_batch as (

)
select * from rf_aub_pmt_reference_table_v2 limit 1;
	
select * from mf_project where proj_alias iN ('ER', 'EVE', 'EB') AND status_id = 'A';
	
select row_number() over (order by a.proj_id, getinteger(b.phase), getinteger(b.block), getinteger(b.lot)) as row_num,
FORMAT('%s-%s', c.proj_alias, b.description) as unit, get_client_name(a.entity_id) as client, a.reference_no
from rf_aub_pmt_reference_table_v2 a
LEFT JOIN mf_unit_info b on TRIM(b.proj_id) = TRIM(a.proj_id) and TRIM(b.pbl_id) = TRIM(a.pbl_id)
LEFT JOIN mf_project c on TRIM(c.proj_id) = TRIM(a.proj_id) 
where a.status_id = 'A'
and a.remitted_to_aub = false
and c.co_id = '02'
and case when trim(a.proj_id) = '015' THEN trim(b.phase) in ('3', '5')
	when trim(a.proj_id) = '018' THEN TRUE
	when trim(a.proj_id) = '019' THEN trim(b.phase) in ('1-B')
	when trim(a.proj_id) = '017' THEN trim(b.phase) in ('2')
	ELSE FALSE END
ORDER BY a.proj_id, getinteger(b.phase), getinteger(b.block), getinteger(b.lot)

select * from rf_aub_pmt_reference_table_v2 where other_lot;

with aub_ref_batch2 as (
select  a.reference_no
from rf_aub_pmt_reference_table_v2 a
LEFT JOIN mf_unit_info b on TRIM(b.proj_id) = TRIM(a.proj_id) and TRIM(b.pbl_id) = TRIM(a.pbl_id)
LEFT JOIN mf_project c on TRIM(c.proj_id) = TRIM(a.proj_id) 
where a.status_id = 'A'
and a.remitted_to_aub = false
and case when trim(a.proj_id) = '015' THEN trim(b.phase) in ('3', '5')
	when trim(a.proj_id) = '018' THEN TRUE
	when trim(a.proj_id) = '019' THEN trim(b.phase) in ('1-B')
	when trim(a.proj_id) = '017' THEN trim(b.phase) in ('2')
	ELSE FALSE END
)

--update rf_aub_pmt_reference_table_v2 a set remitted_to_aub = true, date_edited = now(), edited_by = '900876' from aub_ref_batch2 where a.reference_no = aub_ref_batch2.reference_no

select * from rf_aub_pmt_reference_table_v2 where date_edited::DATE = CURRENT_DATE AND other_lot and remitted_to_aub;

select * from view_card_client_details_debug('8906975909', '015', '2874', 1);

select * from view_card_client_details_debug('0000000031', '019', '6273', 1);
select * from view_card_client_details_debug('9457886935', '015', '12', 1);

select * from rf_aub_pmt_reference_table_v2 where remitted_to_aub = false and status_id = 'A';
