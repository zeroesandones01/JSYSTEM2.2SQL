select y.rplf_no, z.entity_id, z.liq_no, z.rplf_line_no, z.wtax_amt, z.wtax_id, z.entity_type_id, x.*
			from rf_jv_header x 
			left join (select * from rf_liq_header where status_id != 'I') y on x.jv_no = y.jv_no and x.co_id = y.co_id
			right join (select * from rf_liq_detail where status_id != 'I') z on y.liq_no = z.liq_no and z.co_id = y.co_id
			--where x.status_id != 'I'	
			where x.status_id = 'P'
			and x.co_id = '04'
			and y.rplf_no IN ('000002271', '000002273')
			order by y.rplf_no
			
select * from rf_liq_header where rplf_no in ('000002271', '000002273') and co_id = '04';

select wtax_id, wtax_amt, wtax_rate, * from rf_liq_detail where liq_no IN ('000000673', '000000675') AND co_id = '04' and status_id = 'A';

select * from rf_request_detail where rplf_no in ('000002271', '000002273') and co_id = '04' and status_id = 'A';
select * from rf_pv_detail where pv_no in ('000002271', '000002273') and co_id = '04' and status_id = 'A';

select * from rf_pv_header where pv_no = '000002393' and co_id = '04';
select * from rf_entity where entity_id = '4136932939';
select * from rf_entity where 

select * 
from view_EWT_forRemittance_all_v2_debug('04', '','','','', '2024','','10','11','12','11','','', '900876') where c_rplf_no ~*'2316'
