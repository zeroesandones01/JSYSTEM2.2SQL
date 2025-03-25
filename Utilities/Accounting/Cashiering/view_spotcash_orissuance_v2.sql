-- FUNCTION: public.view_spotcash_orissuance_v2(character varying, character varying, character varying, date, date, integer, integer, boolean, character varying)

-- DROP FUNCTION IF EXISTS public.view_spotcash_orissuance_v2(character varying, character varying, character varying, date, date, integer, integer, boolean, character varying);

CREATE OR REPLACE FUNCTION public.view_spotcash_orissuance_v2(
	p_co_id character varying,
	p_proj_id character varying,
	p_phase character varying,
	p_datefrom date,
	p_dateto date,
	p_listfilterindex integer,
	p_datefilterindex integer,
	p_issued boolean,
	p_batch_no character varying)
    RETURNS TABLE(c_tag boolean, c_or_no character varying, c_or_date date, c_name character varying, c_unit character varying, c_partdesc character varying, c_type character varying, c_amount numeric, c_ar_no character varying, c_off_res_date date, c_bank character varying, c_bank_branch_location character varying, c_date_cleared date, c_check_no character varying, c_check_date date, c_pay_rec_id integer, c_jv_no character varying, c_percentage numeric, c_receipt_type character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE v_or RECORD;
	v_ltsDate date; 
	v_boiDate date; 
	v_rf_orlist RECORD;

BEGIN
	/*
	Copied view_spotcash_orissuance by Jari Cruz asof oct 18 2022
	walang nag bago sa conditions neto nag dagdag lang akong batch na parameter
	at para lang to sa pag generate ng report ng mga issued na.
	*/
	
	FOR v_or IN 
	(
		select *, b.lts_date::date, b.boi::date
		from
		(
			select false as tag, (case when p_issued then a.or_no else null end)::varchar as or_no, 
			(case when p_issued then a.or_date else null end)::date as or_date, f.entity_name as name, e.description as unit, 
			i.partdesc, (case when a.pymnt_type = 'B' then 'B.CHECK' else 'A.CASH' end)::varchar(10) as type, a.amount, 
			(case when a.ar_no is null or a.ar_no = '' then a.or_no else a.ar_no end) as ar_no, 
			(select x.tran_date::date from rf_buyer_status x where x.entity_id = a.entity_id and x.proj_id = a.proj_id and x.pbl_id = a.pbl_id and x.seq_no::int = a.seq_no::int and x.status_id = 'A' and x.byrstatus_id = '01') as off_res_date, 
			b.bank_alias as bank, c.bank_branch_location, j.trans_date::date as date_cleared, a.check_no, a.check_date::date as check_date, a.pay_rec_id, 
			a.entity_id, a.proj_id, a.pbl_id, a.seq_no, a.server_id,
			a.pay_part_id, -- added by jari cruz asof 09/02/2022
			(select buyertype from rf_sold_unit 
			 where entity_id = a.entity_id
			 and projcode = a.proj_id
			 and pbl_id = a.pbl_id
			 and seq_no = a.seq_no
			) as buyertype, -- added by jari cruz asof 10/04/2022
			(case when a.si_doc_id is not null then 'SI' else 'OR' end) as receipt_type-- added by jari cruz asof 09/05/2022
			from 
			(
				select x.entity_id, x.proj_id, x.pbl_id, x.seq_no, x.actual_date, x.trans_date, x.pay_part_id, x.pymnt_type, x.bank_id, x.bank_branch_id, x.amount, x.acct_no, x.check_no, 
				x.check_date, x.check_stat_id, x.bounce_reason_id, coalesce(x.si_no,x.or_no) as or_no, coalesce(x.si_date,x.or_date) as or_date, x.ar_no, x.brstn, x.request_no, x.applied_amt, x.cancelled, x.remarks, x.branch_id, x.post_date, 
				x.client_seqno, coalesce(x.si_doc_id,x.or_doc_id) as or_doc_id, x.pr_doc_id, x.status_id, x.wdraw_stat, x.date_wdrawn, x.wdraw_no, x.wdraw_reason, x.repl_wdraw_by, x.date_remitted, x.remit_batch, x.reversed, 
				x.pay_rec_id, x.check_type, x.receipt_id, x.co_id, x.unit_id, x.total_ar_amt, x.created_by, x.date_created, x.refund_date, x.pay_rec_id as from_pay_rec_id, x.server_id, x.si_doc_id
				from rf_payments x 
				where (x.co_id = p_co_id or p_co_id = '')
				and 
				case 
					WHEN x.entity_id in ('1185209653') THEN x.pay_part_id in ('262') -- added by jari cruz asof august 24, 2022, reason special case
					else true
					/*else x.pay_part_id not in ('192', '174', '205', '101', '147', '226', '217','218','251','252','253','254','256','257','258','259','272','273','246','247','262','87', '197', '223', '224','180','198','200','209','220','223','224','241') added 223, 224,'180','198','200','209','220','223','224' by jari cruz asof july 29, 2022
					commented by jari cruz asof sept 14 2022 reason nasa baba na ung filter*/
				end
				and x.pymnt_type = 'A' and trim(x.status_id) = 'A'
				and
				(
					case
						when p_issued = true
							then exists(select * from rf_payments y where y.pay_rec_id::int = x.pay_rec_id::int and (y.remarks ~* 'Late LTS/BOI' or y.remarks ~* 'Late OR Issuance for Good Check'))
								and (coalesce(x.si_doc_id,x.or_doc_id) is not null or trim(coalesce(x.si_doc_id,x.or_doc_id)) != '' 
									 OR x.si_doc_id is not null or trim(x.si_doc_id) != '')--added by jari cruz asof sept 5 2022 reason SI being filtered out
						else NOT exists(select * from rf_payments y where y.pay_rec_id::int = x.pay_rec_id::int and (y.remarks ~* 'Late LTS/BOI' or y.remarks ~* 'Late OR Issuance for Good Check'))
								and (coalesce(x.si_doc_id,x.or_doc_id) is null or trim(coalesce(x.si_doc_id,x.or_doc_id)) = '')
					end
				)
				and
				(
					case
						when p_issued = true
							then
							/*
							modified by jari cruz asof oct 19 2022
							new function sya and separated sa lumang ginagamit pero i note ko parin
							*/
							(CASE WHEN p_batch_no is not null THEN
							 exists(select * from rf_orlist where batch_no = p_batch_no and co_id = p_co_id and pay_rec_id = x.pay_rec_id)
							 ELSE
							 (
								case
									when p_datefilterindex = 0
										then (case when si_doc_id is not null then x.si_date::date --added by jari cruz asof sept 5 2022 para sa may mga SI
											  else coalesce(x.si_date,x.or_date)::date end)
									when p_datefilterindex = 1
										then (select y.date_issued::date from rf_orlist y where y.pay_rec_id::int = x.pay_rec_id::int and status_id = 'A')
								end
							) between p_datefrom and p_dateto 
							 END)
						else true
					end
				)
				and CASE WHEN p_issued THEN TRUE ELSE
				(
					case
						when p_listfilterindex = 0 
							then NOT exists
						(
							select * 
							from rf_sold_unit y 
							where exists
							(
								select * 
								from mf_buyer_type z
								where z.type_id = y.buyertype and z.type_group_id IN ('02', '04')
							)
							and y.entity_id = x.entity_id and y.projcode = x.proj_id 
							and TRIM(y.pbl_id) = TRIM(x.pbl_id) and y.seq_no = x.seq_no and y.status_id = 'A'
						)
						else exists
						(
							select * 
							from rf_sold_unit y 
							where exists
							(
								select * 
								from mf_buyer_type z
								where z.type_id = y.buyertype and z.type_group_id IN ('02', '04')
							)
							and y.entity_id = x.entity_id and y.projcode = x.proj_id 
							and TRIM(y.pbl_id) = TRIM(x.pbl_id) and y.seq_no = x.seq_no and y.status_id = 'A'
						)
					end
				) END
			) a 
			left join mf_bank b on a.bank_id = b.bank_id 
			left join mf_bank_branch c on a.bank_branch_id = c.bank_branch_id 
			left join mf_check_status d on a.check_stat_id = d.checkstat_id  
			left join (select * from mf_unit_info x where x.phase = p_phase or p_phase = '') e on coalesce(a.proj_id,'') = coalesce(e.proj_id,'') and a.pbl_id = e.pbl_id  
			left join rf_entity f on a.entity_id = f.entity_id  
			left join (select * from mf_project x where (x.co_id = p_co_id or p_co_id = '') and (x.proj_id = p_proj_id or p_proj_id = '')) g on a.proj_id = g.proj_id 
			left join mf_check_bounce_reason h on a.bounce_reason_id = h.reason_id 
			left join mf_pay_particular i on a.pay_part_id = i.pay_part_id 
			left join (select distinct on (pay_rec_id) pay_rec_id, date_created as trans_date from rf_check_history where new_checkstat_id = '01' and status_id = 'A' order by pay_rec_id,trans_date desc) j on a.pay_rec_id = j.pay_rec_id::int
			where CASE 
			WHEN a.server_id IS NOT NULL THEN TRUE
			WHEN a.entity_id in ('1185209653') THEN TRUE -- added by jari cruz asof august 24, 2022, reason special case
			ELSE a.pay_part_id in (SELECT pay_part_id FROM mf_pay_particular where receipt_to_issue IN ('01','307') and status_id != 'I') END
			and (case when a.pymnt_type = 'B' then a.check_stat_id = '01' else a.pay_rec_id is not null end) 
			and (a.pbl_id::text, a.seq_no::int) not in (select pbl_id::text, seq_no::int from canceled_accounts) and a.status_id != 'I'
			--and a.or_no not in (select receipt_id from rf_payments where status_id != 'I' and coalesce(receipt_id,'') != '')
				AND CASE WHEN a.server_id IS NOT NULL THEN NULLIF(trim(a.or_no), '') IS NOT /*NOT is added by jari cruz sept 1 2022 reason itsreal client*/ NULL ELSE
            exists (select * from rf_buyer_status x where x.entity_id = a.entity_id and x.proj_id = a.proj_id and x.pbl_id = a.pbl_id and x.seq_no::int = a.seq_no::int and TRIM(x.status_id) = 'A' and TRIM(x.byrstatus_id) = '01')
            END
			union

			select false as tag, (case when p_issued then a.or_no else null end)::varchar as or_no, 
			(case when p_issued then a.or_date else null end)::date as or_date, f.entity_name as name, e.description as unit, 
			i.partdesc, (case when a.pymnt_type = 'B' then 'B.CHECK' else 'A.CASH' end) as type, a.amount, 
			(case when a.ar_no is null or a.ar_no = '' then a.or_no else a.ar_no end) as ar_no, 
			(select x.tran_date::date from rf_buyer_status x where x.entity_id = a.entity_id and x.proj_id = a.proj_id and x.pbl_id = a.pbl_id and x.seq_no::int = a.seq_no::int and x.status_id = 'A' and x.byrstatus_id = '01') as off_res_date, 
			b.bank_alias as bank, c.bank_branch_location, j.trans_date::date as date_cleared, a.check_no, a.check_date::date as check_date, a.pay_rec_id, 
			a.entity_id, a.proj_id, a.pbl_id, a.seq_no, a.server_id,
			a.pay_part_id, -- added by jari cruz asof 09/02/2022
			(select buyertype from rf_sold_unit 
			 where entity_id = a.entity_id
			 and projcode = a.proj_id
			 and pbl_id = a.pbl_id
			 and seq_no = a.seq_no
			) as buyertype, -- added by jari cruz asof 10/04/2022
			(case when a.si_doc_id is not null then 'SI' else 'OR' end) as receipt_type-- added by jari cruz asof 09/05/2022
			from 
			(
				select x.entity_id, x.proj_id, x.pbl_id, x.seq_no, x.actual_date, x.trans_date, x.pay_part_id, x.pymnt_type, x.bank_id, x.bank_branch_id, x.amount, x.acct_no, x.check_no, 
				x.check_date, x.check_stat_id, x.bounce_reason_id, coalesce(x.si_no,x.or_no) as or_no, coalesce(x.si_date,x.or_date) as or_date, x.ar_no, x.brstn, x.request_no, x.applied_amt, x.cancelled, x.remarks, x.branch_id, x.post_date, 
				x.client_seqno, coalesce(x.si_doc_id,x.or_doc_id) as or_doc_id, x.pr_doc_id, x.status_id, x.wdraw_stat, x.date_wdrawn, x.wdraw_no, x.wdraw_reason, x.repl_wdraw_by, x.date_remitted, x.remit_batch, x.reversed, 
				x.pay_rec_id, x.check_type, x.receipt_id, x.co_id, x.unit_id, x.total_ar_amt, x.created_by, x.date_created, x.refund_date, x.pay_rec_id as from_pay_rec_id, x.server_id, x.si_doc_id
				from rf_payments x 
				where (x.co_id = p_co_id or p_co_id = '')
				/*and x.pay_part_id not in ('192', '174', '205', '101', '147', '226', '217','218','251','252','253','254','256','257','258','259','272','273','246','247','262','87', '197', '223', '224','180','198','200','209','220','223','224','241') -- added 223, 224,'180','198','200','209','220','223','224' by jari cruz asof july 29, 2022
				commented by jari cruz asof sept 14 2022 reason nasa baba na ung filter*/
				and x.pymnt_type = 'B' and trim(x.status_id) = 'A'
				and
				(
					case
						when p_issued = true
							then exists(select * from rf_payments y where y.pay_rec_id::int = x.pay_rec_id::int and (y.remarks ~* 'Late LTS/BOI' or y.remarks ~* 'Late OR Issuance for Good Check'))
								and (coalesce(x.si_doc_id,x.or_doc_id) is not null or trim(coalesce(x.si_doc_id,x.or_doc_id)) != '')
						else NOT exists(select * from rf_payments y where y.pay_rec_id::int = x.pay_rec_id::int and (y.remarks ~* 'Late LTS/BOI' or y.remarks ~* 'Late OR Issuance for Good Check'))
								and (coalesce(x.si_doc_id,x.or_doc_id) is null or trim(coalesce(x.si_doc_id,x.or_doc_id)) = '')
					end
				)
				and
				(
					case
						when p_issued = true
							then
							/*
							modified by jari cruz asof oct 19 2022
							new function sya and separated sa lumang ginagamit pero i note ko parin
							*/
							(CASE WHEN p_batch_no is not null THEN
							 exists(select * from rf_orlist where batch_no = p_batch_no and co_id = p_co_id and pay_rec_id = x.pay_rec_id)
							 ELSE
							 (
								case
									when p_datefilterindex = 0
										then coalesce(x.si_date,x.or_date)::date
									when p_datefilterindex = 1
										then (select y.date_issued::date from rf_orlist y where y.pay_rec_id::int = x.pay_rec_id::int AND status_id = 'A')
								end
							) between p_datefrom and p_dateto 
							 END)
						else true
					end
				)
				and CASE WHEN p_issued THEN TRUE ELSE
				(
					case
						when p_listfilterindex = 0 and x.pay_part_id = '033'
						then 
						 exists
						(
							select * 
							from rf_sold_unit y 
							where exists
							(
								select * 
								from mf_buyer_type z
								where z.type_id = y.buyertype and z.type_group_id IN ('02', '04','03') -- added 03 by jari cruz asof oct 5 2022 ndi kasi nasasama mga cash and deferred
							)
							and y.entity_id = x.entity_id and y.projcode = x.proj_id 
							and TRIM(y.pbl_id) = TRIM(x.pbl_id) and y.seq_no = x.seq_no and y.status_id = 'A'
						)
						when p_listfilterindex = 0
							then 
						NOT exists
						(
							select * 
							from rf_sold_unit y 
							where exists
							(
								select * 
								from mf_buyer_type z
								where z.type_id = y.buyertype and z.type_group_id IN ('04') -- removed 02 by jari cruz asof oct 5 2022 ndi kasi ndi lumilitaw mga in house
							)
							and y.entity_id = x.entity_id and y.projcode = x.proj_id 
							and TRIM(y.pbl_id) = TRIM(x.pbl_id) and y.seq_no = x.seq_no and y.status_id = 'A'
						)
					
					
						else exists
						(
							select * 
							from rf_sold_unit y 
							where exists
							(
								select * 
								from mf_buyer_type z
								where z.type_id = y.buyertype and z.type_group_id IN ('02', '04','03') -- added 03 by jari cruz asof oct 5 2022 ndi kasi nasasama mga cash and deferred
							)
							and y.entity_id = x.entity_id and y.projcode = x.proj_id 
							and TRIM(y.pbl_id) = TRIM(x.pbl_id) and y.seq_no = x.seq_no and y.status_id = 'A'
						)
					end
				) END
			) a 
			left join mf_bank b on a.bank_id = b.bank_id 
			left join mf_bank_branch c on a.bank_branch_id = c.bank_branch_id 
			left join mf_check_status d on a.check_stat_id = d.checkstat_id  
			left join mf_unit_info e on a.proj_id = e.proj_id and a.pbl_id = e.pbl_id  
			left join rf_entity f on a.entity_id = f.entity_id  
			left join (select * from mf_project x where (x.co_id = p_co_id or p_co_id = '') and (x.proj_id = p_proj_id or p_proj_id = '')) g on a.proj_id = g.proj_id 
			left join mf_check_bounce_reason h on a.bounce_reason_id = h.reason_id 
			left join mf_pay_particular i on a.pay_part_id = i.pay_part_id 
			left join (select distinct on (pay_rec_id) pay_rec_id, date_created as trans_date from rf_check_history where new_checkstat_id = '01' and status_id = 'A' order by pay_rec_id,trans_date desc) j on a.pay_rec_id = j.pay_rec_id::int
			where CASE WHEN a.server_id IS NOT NULL THEN TRUE ELSE a.pay_part_id in (SELECT pay_part_id FROM mf_pay_particular where receipt_to_issue IN ('01','307') and status_id != 'I') END 
			and (case when a.pymnt_type = 'B' then a.check_stat_id = '01' else a.pay_rec_id is not null end) 
			and (a.pbl_id::text, a.seq_no::int) not in (select pbl_id::text, seq_no::int from canceled_accounts) and a.status_id != 'I'
			--and a.or_no not in (select receipt_id from rf_payments where status_id != 'I' and coalesce(receipt_id,'') != '')
			/*
			commented by jari cruz asof oct 11 2022
			reason may mga good check na ndi lumilitaw
			AND CASE WHEN a.server_id IS NOT NULL THEN NULLIF(trim(a.or_no), '') IS NOT /*NOT is added by jari cruz sept 1 2022 reason itsreal client*/ NULL
			OR a.pay_rec_id in (256611,476767,495919,237630,522390) -- added by jari cruz asof sept 14 2022 reason uploaded from itsreal
			ELSE
            exists (select * from rf_buyer_status x where x.entity_id = a.entity_id and x.proj_id = a.proj_id and x.pbl_id = a.pbl_id and x.seq_no::int = a.seq_no::int and x.status_id = 'A' and x.byrstatus_id = '01')
            END
			*/
			and exists (select * from rf_buyer_status x where x.entity_id = a.entity_id and x.proj_id = a.proj_id and x.pbl_id = a.pbl_id and x.seq_no::int = a.seq_no::int and x.status_id = 'A' and trim(x.byrstatus_id) = '01')
			/*eto lang tinira ko*/
			union

			select false as tag, (case when p_issued then a.or_no else null end)::varchar as or_no, 
			(case when p_issued then a.or_date else null end)::date as or_date, f.entity_name as name, e.description as unit, 
			i.partdesc, (case when a.pymnt_type = 'B' then 'B.CHECK' else 'A.CASH' end) as type, a.amount, 
			(case when a.ar_no is null or a.ar_no = '' then a.or_no else a.ar_no end) as ar_no, 
			(select x.tran_date::date from rf_buyer_status x where x.entity_id = a.entity_id and x.proj_id = a.proj_id and x.pbl_id = a.pbl_id and x.seq_no::int = a.seq_no::int and x.status_id = 'A' and x.byrstatus_id = '01') as off_res_date, 
			b.bank_alias as bank, c.bank_branch_location, j.trans_date::date as date_cleared, a.check_no, a.check_date::date as check_date, a.pay_rec_id, 
			a.entity_id, a.proj_id, a.pbl_id, a.seq_no, a.server_id,
			a.pay_part_id, -- added by jari cruz asof 09/02/2022
			(select buyertype from rf_sold_unit 
			 where entity_id = a.entity_id
			 and projcode = a.proj_id
			 and pbl_id = a.pbl_id
			 and seq_no = a.seq_no
			) as buyertype, -- added by jari cruz asof 10/04/2022
			(case when a.si_doc_id is not null then 'SI' else 'OR' end) as receipt_type-- added by jari cruz asof 09/05/2022
			from 
			(
				select x.entity_id, x.proj_id, x.pbl_id, x.seq_no, x.actual_date, x.trans_date, x.pay_part_id, x.pymnt_type, x.bank_id, x.bank_branch_id, x.amount, x.acct_no, x.check_no, 
				x.check_date, a.check_stat_id, x.bounce_reason_id, coalesce(x.si_no,x.or_no) as or_no, coalesce(x.si_date,x.or_date) as or_date, x.ar_no, x.brstn, x.request_no, x.applied_amt, x.cancelled, x.remarks, x.branch_id, x.post_date, 
				x.client_seqno, coalesce(x.si_doc_id,x.or_doc_id) as or_doc_id, x.pr_doc_id, x.status_id, x.wdraw_stat, x.date_wdrawn, x.wdraw_no, x.wdraw_reason, x.repl_wdraw_by, x.date_remitted, x.remit_batch, x.reversed, 
				x.pay_rec_id, x.check_type, x.receipt_id, x.co_id, x.unit_id, x.total_ar_amt, x.created_by, x.date_created, x.refund_date, a.pay_rec_id as from_pay_rec_id, x.server_id, x.si_doc_id
				from rf_payments x 
				inner join rf_payments a on x.receipt_id = a.or_no and x.check_no = a.check_no and x.check_date::Date = a.check_date
				where (x.co_id = p_co_id or p_co_id = '')
				/*and x.pay_part_id not in ('192', '174', '205', '101', '147', '226', '217','218','251','252','253','254','256','257','258','259','272','273','246','247','262','87', '197', '223', '224','180','198','200','209','220','223','224','241') -- added 223, 224,'180','198','200','209','220','223','224' by jari cruz asof july 29, 2022
				commented by jari cruz asof sept 14 2022 reason nasa baba na ung filter*/
				and x.pymnt_type = 'B' and trim(x.status_id) = 'A' and (x.receipt_id is not null or x.receipt_id != '')
				and
				(
					case
						when p_issued = true
							then exists(select * from rf_payments y where y.pay_rec_id::int = x.pay_rec_id::int and (y.remarks ~* 'Late LTS/BOI' or y.remarks ~* 'Late OR Issuance for Good Check'))
								and (coalesce(x.si_doc_id,x.or_doc_id) is not null or trim(coalesce(x.si_doc_id,x.or_doc_id)) != '')
						else NOT exists(select * from rf_payments y where y.pay_rec_id::int = x.pay_rec_id::int and (y.remarks ~* 'Late LTS/BOI' or y.remarks ~* 'Late OR Issuance for Good Check'))
								and (coalesce(x.si_doc_id,x.or_doc_id) is null or trim(coalesce(x.si_doc_id,x.or_doc_id)) = '')
					end
				)
				and
				(
					case
						when p_issued = true
							then
							/*
							modified by jari cruz asof oct 19 2022
							new function sya and separated sa lumang ginagamit pero i note ko parin
							*/
							(CASE WHEN p_batch_no is not null THEN
							 exists(select * from rf_orlist where batch_no = p_batch_no and co_id = p_co_id and pay_rec_id = x.pay_rec_id)
							 ELSE
							 (
								case
									when p_datefilterindex = 0
										then coalesce(x.si_date,x.or_date)::date
									when p_datefilterindex = 1
										then (select y.date_issued::date from rf_orlist y where y.pay_rec_id::int = x.pay_rec_id::int AND status_id = 'A')
								end
							) between p_datefrom and p_dateto 
							 END)
						else true
					end
				)
				and 
				(
					case
					when p_listfilterindex = 0 and x.pay_part_id = '033'
						then 
						 exists
						(
							select * 
							from rf_sold_unit y 
							where exists
							(
								select * 
								from mf_buyer_type z
								where z.type_id = y.buyertype and z.type_group_id IN ('02', '04')
							)
							and y.entity_id = x.entity_id and y.projcode = x.proj_id 
							and TRIM(y.pbl_id) = TRIM(x.pbl_id) and y.seq_no = x.seq_no and y.status_id = 'A'
						)
					
						when p_listfilterindex = 0
							then 
						NOT exists
						(
							select * 
							from rf_sold_unit y 
							where exists
							(
								select * 
								from mf_buyer_type z
								where z.type_id = y.buyertype and z.type_group_id IN ('02', '04')
							)
							and y.entity_id = x.entity_id and y.projcode = x.proj_id 
							and TRIM(y.pbl_id) = TRIM(x.pbl_id) and y.seq_no = x.seq_no and y.status_id = 'A'
						)
						else exists
						(
							select * 
							from rf_sold_unit y 
							where exists
							(
								select * 
								from mf_buyer_type z
								where z.type_id = y.buyertype and z.type_group_id IN ('02', '04')
							)
							and y.entity_id = x.entity_id and y.projcode = x.proj_id 
							and TRIM(y.pbl_id) = TRIM(x.pbl_id) and y.seq_no = x.seq_no and y.status_id = 'A'
						)
					end
				)
			) a 
			left join mf_bank b on a.bank_id = b.bank_id 
			left join mf_bank_branch c on a.bank_branch_id = c.bank_branch_id 
			left join mf_check_status d on a.check_stat_id = d.checkstat_id  
			left join mf_unit_info e on a.proj_id = e.proj_id and a.pbl_id = e.pbl_id  
			left join rf_entity f on a.entity_id = f.entity_id  
			left join (select * from mf_project x where (x.co_id = p_co_id or p_co_id = '') and (x.proj_id = p_proj_id or p_proj_id = '')) g on a.proj_id = g.proj_id 
			left join mf_check_bounce_reason h on a.bounce_reason_id = h.reason_id 
			left join mf_pay_particular i on a.pay_part_id = i.pay_part_id 
			left join (select distinct on (pay_rec_id) pay_rec_id, date_created as trans_date from rf_check_history where new_checkstat_id = '01' and status_id = 'A' order by pay_rec_id,trans_date desc) j on a.from_pay_rec_id = j.pay_rec_id::int
			where CASE WHEN a.server_id IS NOT NULL THEN TRUE ELSE a.pay_part_id in (SELECT pay_part_id FROM mf_pay_particular where receipt_to_issue IN ('01','307') and status_id != 'I') END
			and (case when a.pymnt_type = 'B' then a.check_stat_id = '01' else a.pay_rec_id is not null end) 
			and (a.pbl_id::text, a.seq_no::int) not in (select pbl_id::text, seq_no::int from canceled_accounts) and a.status_id != 'I'
			--and a.or_no not in (select receipt_id from rf_payments where status_id != 'I' and coalesce(receipt_id,'') != '')
			AND CASE WHEN a.server_id IS NOT NULL THEN NULLIF(trim(a.or_no), '') IS NOT /*NOT is added by jari cruz sept 1 2022 reason itsreal client*/ NULL ELSE
            exists (select * from rf_buyer_status x where x.entity_id = a.entity_id and x.proj_id = a.proj_id and x.pbl_id = a.pbl_id and x.seq_no::int = a.seq_no::int and x.status_id = 'A' and x.byrstatus_id = '01')
            END
		) a
		left join
		(
			select x.lts_date::date, boi::date, z.pay_rec_id::int
			from mf_sub_project x 
			inner join mf_unit_info y on y.proj_id = x.proj_id and y.phase = x.phase and y.sub_proj_id = x.sub_proj_id
			inner join rf_payments z on y.proj_id = z.proj_id and y.pbl_id = z.pbl_id 
		) b on a.pay_rec_id::int = b.pay_rec_id::int
		where p_issued or not exists(select * from rf_orlist x where a.pay_rec_id::int = x.pay_rec_id::int AND status_id = 'A')
		order by a.type, a.name, a.unit, a.check_date, a.check_no

	)
	LOOP

		c_receipt_type	:= v_or.receipt_type;
		c_tag			:= exists(select * from rf_orlist where pay_rec_id::int = v_or.pay_rec_id::int AND status_id = 'A'); 
		c_or_no			:= v_or.or_no; 

		v_ltsDate		:= v_or.lts_date; 
		v_boiDate		:= v_or.boi;
		
		raise info ''; 
		raise info 'v_ltsDate: %', v_ltsDate; 
		raise info 'v_boiDate: %', v_boiDate;
        RAISE INFO 'Pay RecID: %', v_or.pay_rec_id::int;
		RAISE INFO 'index: %', p_listfilterindex;
		
		c_or_date		:= coalesce(v_or.or_date, greatest(v_ltsDate, v_boiDate, v_or.off_res_date, v_or.date_cleared));
		
		IF p_listfilterindex = 1 and p_batch_no is null THEN
			c_or_date := now();
		END IF;

		c_name			:= v_or.name; 
		c_unit			:= v_or.unit; 
		c_partdesc		:= (select particulars from mf_pay_particular where partdesc = v_or.partdesc and status_id = 'A'); 
		c_type			:= v_or.type; 
		c_amount		:= v_or.amount; 
		c_ar_no			:= v_or.ar_no; 
		c_off_res_date		:= v_or.off_res_date; 
		c_bank			:= v_or.bank; 
		c_bank_branch_location	:= v_or.bank_branch_location; 
		c_date_cleared		:= v_or.date_cleared; 
		c_check_no		:= v_or.check_no; 
		c_check_date		:= v_or.check_date; 
		c_pay_rec_id		:= v_or.pay_rec_id; 
		
		IF c_pay_rec_id IN (237628) THEN
			c_date_cleared := now();
		END IF;

		c_jv_no			:= 
		(
			select x.jv_no 
			from rf_jv_header x 
			where x.status_id != 'D' and position(c_or_no in x.remarks) > 0
			and x.remarks ~* ('TO RECORD THE ADJUSTMENTS FOR THE COLLECTION DATE ' || c_or_date::date::varchar)::varchar limit 1
		); 

		c_jv_no		:= coalescE(c_jv_no, ''); 
		c_percentage	:= (select sp_compute_payment_percentage(v_or.entity_id, v_or.proj_id, v_or.pbl_id, v_or.seq_no, v_or.pay_rec_id) from rf_payments x where x.pay_rec_id::int = v_or.pay_rec_id::int); 

		/*
		if (c_or_date::Date < now()::date or c_partdesc = 'SPOTCASH') then
			RETURN NEXT;
		end if;
		*/
		
		
		
 		/* commented by jari cruz as of sept 2 2022 reason, filter if p_listfilterindex/spotcash & deffered then return only if paypart id is for spot cash deffered
		if c_or_date is not null and (v_ltsDate is not null or v_boiDate is not null) then
			RETURN NEXT;
 		end if;*/ 
		
		if p_issued = false and p_listfilterindex = 1 then
		/*
		added get_group_id(v_or.buyertype) = '03' by jari cruz asof oct 12 2022
		reason mga cash buyer daw lalabas sa index 1 (spotcash & deffered)
		*/
			if get_group_id(v_or.buyertype) = '03' AND trim(v_or.pay_part_id) in ('033','262','263') then
				RETURN NEXT;
			end if;
		else
			if p_issued = false AND get_group_id(v_or.buyertype) != '03' AND trim(v_or.pay_part_id) in ('033','040','041','042','185','260','262','263') then -- as of sept 13 2022 nilipat ko nlng kasi parepareho lang naman condition dun sa taas... added '182','163' sept 13 2022
				-- added 195,261 by jari cruz asof sept 19 2022
				RETURN NEXT;
			end if;
		end if;
		
		IF p_issued THEN
			RETURN NEXT;
		END IF;
	END LOOP;

END;
$BODY$;

ALTER FUNCTION public.view_spotcash_orissuance_v2(character varying, character varying, character varying, date, date, integer, integer, boolean, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_spotcash_orissuance_v2(character varying, character varying, character varying, date, date, integer, integer, boolean, character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_spotcash_orissuance_v2(character varying, character varying, character varying, date, date, integer, integer, boolean, character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.view_spotcash_orissuance_v2(character varying, character varying, character varying, date, date, integer, integer, boolean, character varying) TO postgres;

