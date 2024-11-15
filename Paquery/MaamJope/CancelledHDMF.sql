
SELECT g.entity_name, b.proj_alias, c.description, e.actual_date::DATE as "lnrel_date", f.actual_date::dATE as "cancelled_hdmf", d.hlid_no, 
a.proj_server, a.server_id
FROM rf_sold_unit a
LEFT JOIN mf_project b on TRIM(b.proj_id) = TRIM(a.projcode)
LEFT JOIN mf_unit_info c on TRIM(c.proj_id) = TRIM(a.projcode) and TRIM(c.pbl_id) = TRIM(a.pbl_id)
LEFT JOIN rf_hlid_no d on TRIM(d.entity_id) = TRIM(a.entity_id) and TRIM(d.proj_id) = TRIM(a.projcode) and TRIM(d.pbl_id) = TRIM(a.pbl_id) and d.seq_no = a.seq_no and TRIM(d.status_id) = 'A'
left join rf_buyer_status e on trim(e.entity_id) = trim(a.entity_id) and trim(e.proj_id) = trim(a.projcode) and trim(e.pbl_id) = trim(a.pbl_id) and e.seq_no = a.seq_no and trim(e.byrstatus_id) = '32' AND trim(e.status_id) = 'A'
LEFT JOIN rf_buyer_status f on trim(f.entity_id) = trim(a.entity_id) and trim(f.proj_id) = trim(a.projcode) and trim(f.pbl_id) = trim(a.pbl_id) and f.seq_no = a.seq_no and trim(f.byrstatus_id) = '76' AND trim(f.status_id) = 'A'
left join rf_entity g on g.entity_id = a.entity_id
--left join rf_entity_id_no h on h.entity_id = a.entity_id
where a.currentstatus != '02'
AND a.status_id = 'A'
and c.proj_server = 'old'
--and d.hlid_no is null
and f.actual_date is not null
order by b.proj_alias, getinteger(c.phase), getinteger(c.block), getinteger(c.lot) limit 1;

select *
from mf_buyer_status where status_desc ~*'cancelled';

select * from rf_entity_id_no;

select *
from hs_sold_other_lots a
where a.status_id = 'A'
AND exists (select *
		    from rf_hlid_no
		    WHERE TRIM(entity_id) = TRIM(a.entity_id)
		    and TRIM(proj_id) = TRIM(a.proj_id)
		    and TRIM(pbl_id) = TRIM(a.oth_pbl_id)
		    and seq_no = a.seq_no
		    and status_id = 'A');

select * from rf_hlid_no limit 1;
