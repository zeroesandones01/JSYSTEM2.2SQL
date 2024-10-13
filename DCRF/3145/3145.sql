
select *
from mf_buyer_type;

select * from mf_pay_particular where particulars ~*'tcost'

SELECT * FROM mf_buyer_status where status_desc ~*'full settle';
select * from mf_product_model limit 1;
8:47

select a.entity_id, a.projcode, a.pbl_id, a.seq_no, get_client_name(a.entity_id), b.proj_alias, c.phase, c.block, c.lot, d.model_desc, e.net_sprice
from rf_sold_unit a
LEFT JOIN mf_project b on b.proj_id= a.projcode
LEFT JOIN mf_unit_info c on trim(c.proj_id) = trim(a.projcode) and trim(c.pbl_id) = trim(a.pbl_id)
LEFT JOIN mf_product_model d on trim(d.model_id) = trim(a.model_id) and coalesce(d.server_id, '') = coalesce(a.server_id) and coalesce(d.proj_server, '') = coalesce(a.proj_server, '')
LEFT JOIN rf_client_price_history e on trim(e.entity_id) = trim(a.entity_id) and trim(e.proj_id) = trim(a.projcode) and trim(e.pbl_id) = trim(a.pbl_id) and e.seq_no = a.seq_no and trim(e.status_id) = 'A'
where a.currentstatus != '02'
AND a.status_id = 'A'
AND get_group_id(trim(a.buyertype)) = '02'
and case when nullif('02', 'null') is null then true else b.co_id = '02' end
and case when nullif('null', 'null') is null then true else a.projcode = 'null' end
and case when nullif('null', 'null') is null then true else c.phase = 'null' end
AND EXISTS (SELECT *
 			FROM rf_buyer_status
			where trim(entity_id) = trim(a.entity_id)
			and trim(proj_id) = trim(a.projcode)
			and trim(pbl_id) = trim(a.pbl_id)
			and seq_no = a.seq_no
			and trim(byrstatus_id) = '27'
			AND trim(status_id) = 'A')
and not exists (SELECT *
 				FROM rf_payments
				where trim(entity_id) = a.entity_id
				and trim(proj_id) = trim(a.projcode)
				and trim(pbl_id) = trim(a.pbl_id)
				and seq_no = a.seq_no
				and TRIM(pay_part_id) = '182'
				AND status_id = 'A')
AND NOT EXISTS (SELECT *
 			FROM rf_buyer_status
			where trim(entity_id) = trim(a.entity_id)
			and trim(proj_id) = trim(a.projcode)
			and trim(pbl_id) = trim(a.pbl_id)
			and seq_no = a.seq_no
			and trim(byrstatus_id) IN ('1D', '103')
			AND trim(status_id) = 'A')

SELECT * FROM view_unilateral_gen_monitoring('01', 'null', 'null');

select * from rf_printed_documents where entity_id = '5705118161';

select * from rf_tct_taxdec_monitoring_hd;

