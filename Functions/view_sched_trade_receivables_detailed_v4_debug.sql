-- FUNCTION: public.view_sched_trade_receivables_detailed_v4_debug(character varying, character varying, character varying, timestamp without time zone)

-- DROP FUNCTION IF EXISTS public.view_sched_trade_receivables_detailed_v4_debug(character varying, character varying, character varying, timestamp without time zone);

CREATE OR REPLACE FUNCTION public.view_sched_trade_receivables_detailed_v4_debug(
	p_co_id character varying,
	p_proj_id character varying,
	p_phase_no character varying,
	p_asof_date timestamp without time zone)
    RETURNS TABLE(c_stage character varying, c_pbl character varying, c_client_name character varying, c_saleable_area numeric, c_gsp numeric, c_discount numeric, c_vat numeric, c_nsp numeric, c_realized_date timestamp without time zone, c_coll_int numeric, c_col_prin numeric, c_fulldp_date timestamp without time zone, c_turnover_date timestamp without time zone, c_movein_date timestamp without time zone, c_docs_com_date timestamp without time zone, c_b_type character varying, c_pay_stage character varying, c_days_past_due integer, c_res_amt_tot numeric, c_proc_fee_tot numeric, c_dp_amt_tot numeric, c_res_amt_paid numeric, c_proc_fee_paid numeric, c_dp_amt_paid numeric, c_amt_not_collected_due numeric, c_amt_not_collected_not_due numeric, c_hse_model character varying, c_ntc_date timestamp without time zone, c_contract_no character varying, c_contract_amt numeric, c_start_date timestamp without time zone, c_end_date timestamp without time zone, c_perc_comp numeric, c_date_comp timestamp without time zone, c_phase character varying, c_block character varying, c_lot character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    IMMUTABLE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

  v_rec RECORD;
  v_tr_status RECORD;
  v_or_status RECORD;
  v_dp_status RECORD;
  v_fs_status RECORD;
  v_realized_status RECORD;
  v_lf_status RECORD;
  v_lr_status RECORD;
  v_to_status RECORD;
  v_ofsaub_status RECORD;
  v_movein_status RECORD;
  v_docs_com_status RECORD;
  v_fsmri_status RECORD;
  
  v_rf_client_price_history RECORD;
  v_rf_client_ledger_latest RECORD;
  v_rf_client_schedule RECORD;
  v_mf_client_ledger_part RECORD;
  v_rf_client_schedule_new_date RECORD;
  v_mf_client_ledger_part_new RECORD;
  v_rf_client_schedule_res RECORD;
  v_rf_client_ledger_res RECORD;
  
  v_balance numeric;
  v_interest numeric;
  v_days_past_due numeric;
  v_amount_paid_percentage numeric;
  v_rf_client_ledger_dp_sum numeric;
  
  v_Co_NTP_Header record;
  v_Contract_No varchar;
  v_NTP_No varchar;
  v_Original_Contract record;
  v_Last_Accomplishment record;
  v_Co_NTP_Detail record;
  
  v_assumed_to_status RECORD;

BEGIN
--ORIGINAL FUNCTION view_sched_trade_receivables_detailed_v3
FOR v_rec IN 
(
	select
	trim(c.entity_name) as entity_name,
	trim(b.entity_id) as entity_id,
	trim(b.projcode) as proj_id,
	trim(b.pbl_id) as pbl_id,
	b.seq_no as seq_no,
	b.model_id,
	b.buyertype,
	a.status_id as mf_unit_info_status_id,
	get_group_id(b.buyertype) as group_id,
	a.description as description,
	a.phase as phase,
	a.block as block,
	a.lot as lot,
	a.lotarea as saleable_area,
	a.server_id,
	a.proj_server, a.rec_id
	from mf_unit_info a
	left join rf_sold_unit b on a.pbl_id = b.pbl_id and a.proj_id = b.projcode
	left join rf_entity c on b.entity_id = c.entity_id
	/*
	MODIFIED BY JARI CRUZ ASOF FEB 1 2023
	where (case when p_proj_id = '' or p_proj_id is null then true else a.proj_id = p_proj_id end)
	*/
	where 
	(
		case 
		when p_proj_id = '' or p_proj_id is null then true 
		when p_proj_id = '017' then a.proj_id IN ('017','008') 
		else a.proj_id = p_proj_id end
	)
	AND a.rec_id IN (26248,26249,26250,26251,26252)
	and (case when p_phase_no = '' or p_phase_no is null then true else a.sub_proj_id = p_phase_no end)
	and case when a.rec_id IN (26248,26249,26250,26251,26252) then true else  trim(b.status_id) = 'A' end
	and case when a.rec_id IN (26248,26249,26250,26251,26252) then true else trim(b.currentstatus) != '02' end
	and not exists (SELECT *
				    FROM rf_uploaded_card_from_itsreal 
				    where entity_id = b.entity_id 
				    and proj_id = b.projcode 
				    and pbl_id = b.pbl_id
				    and seq_no = b.seq_no 
				    )
	--and b.entity_id = '0484448069'
) 
LOOP

	raise info 'add loop';

	select into v_rf_client_price_history * from rf_client_price_history 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id 
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no 
	and status_id = 'A'
	and coalesce(server_id,'') = coalesce(v_rec.server_id,'')
	and coalesce(proj_server,'') = coalesce(v_rec.proj_server,'')
	order by tran_date desc;

	select into v_rf_client_ledger_latest * from rf_client_ledger 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id 
	and pbl_id = v_rec.pbl_id 
	and seq_no = v_rec.seq_no 
	and status_id = 'A' 
	and coalesce(server_id,'') = coalesce(v_rec.server_id,'')
	and coalesce(proj_server,'') = coalesce(v_rec.proj_server,'')
	and date_paid::date <= p_asof_date::date 
	order by appl_date desc,sched_date desc;

	select into v_rf_client_schedule * from rf_client_schedule 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id 
	and pbl_id = v_rec.pbl_id 
	and seq_no = v_rec.seq_no 
	and status_id = 'A' 
	and coalesce(server_id,'') = coalesce(v_rec.server_id,'')
	and coalesce(proj_server,'') = coalesce(v_rec.proj_server,'')
	and scheddate::date = v_rf_client_ledger_latest.sched_date::date
	order by scheddate desc;

	select into v_mf_client_ledger_part * from mf_client_ledger_part where trim(part_id) = trim(v_rf_client_schedule.part_id) and status_id = 'A';

	select into v_tr_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('17')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_or_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('01')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_dp_status * from rf_buyer_status 
	where trim(entity_id) = trim(v_rec.entity_id) 
	and trim(proj_id) = trim(v_rec.proj_id)
	and trim(pbl_id) = trim(v_rec.pbl_id)
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('19')
	and trim(status_id) = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_fs_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('27','135','138')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_realized_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('22')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_lf_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('31')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_lr_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('32', '1F')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_ofsaub_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('1D','103')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_to_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('39')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_assumed_to_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('141')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_movein_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('36')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1;

	select into v_docs_com_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('18')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1; 

	select into v_fsmri_status * from rf_buyer_status 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id
	and pbl_id = v_rec.pbl_id
	and seq_no = v_rec.seq_no
	and trim(byrstatus_id) in ('138')
	and status_id = 'A'
	and tran_date::date <= p_asof_date::date
	order by tran_date
	limit 1; 

	c_phase := v_rec.phase;
	c_block := v_rec.block;
	c_lot := v_rec.lot;
	c_pbl := (select get_merge_unit_desc_v3_other(v_rec.entity_id, v_rec.proj_id, v_rec.pbl_id, v_rec.seq_no));
	c_client_name := trim(v_rec.entity_name);
	c_saleable_area := (case when v_rec.saleable_area = 0 then null::numeric else coalesce((select lot_area from hs_sold_other_lots where pbl_id = v_rec.pbl_id and proj_id = v_rec.proj_id and entity_id = v_rec.entity_id and seq_no = v_rec.seq_no and status_id = 'A'),v_rec.saleable_area) end); 
	c_GSP := (case when v_rf_client_price_history.gross_sprice = 0 then null::numeric else v_rf_client_price_history.gross_sprice end);
	c_discount := (case when v_rf_client_price_history.discount = 0 then null::numeric else v_rf_client_price_history.discount end);
	IF v_rec.entity_id = '9903083844' AND v_rec.pbl_id = '6494' THEN /*added by jari cruz asof nov 7 2022, reason SPECIAL CASE (CUARESMA, CARLO PAULO LANSANG)*/
		c_GSP := 1891624.00::numeric;
		c_discount := 189162.40::numeric;
	END IF;
	c_vat := (case when v_rf_client_price_history.vat = 0 then null::numeric else v_rf_client_price_history.vat end);
	c_NSP := (case when v_rf_client_price_history.net_sprice = 0 then null::numeric else v_rf_client_price_history.net_sprice end);
	c_realized_date := v_realized_status.tran_date;
	c_coll_int := 
	(
		SELECT SUM(a.amount) 
		from rf_client_ledger a
		where a.entity_id = v_rec.entity_id 
		and a.proj_id = v_rec.proj_id 
		and a.pbl_id = v_rec.pbl_id 
		and a.seq_no = v_rec.seq_no 
		and a.part_id in ('001', '015') 
		and a.status_id = 'A' 
		and coalesce(a.server_id, '') = COALESCE(v_rec.server_id, '') 
		and coalesce(a.proj_server, '') = coalesce(v_rec.proj_server, '')
		and pay_rec_id in (select pay_rec_id from rf_payments where trans_date::date<= p_asof_date)
	);
	
	IF v_rec.server_id is not null THEN
		c_coll_int := coalesce(c_coll_int,0.00) + coalesce((select total_int from rf_client_ledger_itsreal_lumpsum a left join rf_entity_itsreal b on a.entity_id = b.entity_id and b.proj_server = v_rec.proj_server where TRIM(b.entity_name) = TRIM(v_rec.entity_name) and proj_id = v_rec.proj_id and pbl_id = v_rec.pbl_id and seq_no = v_rec.seq_no),0.00);
	END IF;

	IF v_tr_status.tran_date::date > '2022-07-04'::date OR v_rec.server_id is null then
		v_balance := (coalesce(c_nsp,0) - (select sum(coalesce(c_principal,0) + coalesce(c_dp,0)) from view_card_ledger_with_moratorium_v2(v_rec.entity_id, v_rec.proj_id, v_rec.pbl_id, v_rec.seq_no , false) where c_trans_date::date <= p_asof_date::date));
	ELSE
		v_balance := (SELECT coalesce(xb.c_balance,0) FROM view_card_ledger_with_moratorium_v2(v_rec.entity_id, v_rec.proj_id, v_rec.pbl_id, v_rec.seq_no , false) xb where c_trans_date::date <= p_asof_date ORDER BY c_sched_date DESC,xb.c_balance LIMIT 1);
	END IF;

	RAISE INFO 'c_balance: %', v_balance;

	IF v_balance is null THEN
		v_balance := 0;
	END IF;
	
	/*
	IF v_rec.group_id IN ('04') THEN
		v_balance := 
		COALESCE((select loanable_amount from rf_pagibig_computation where entity_id = v_rec.entity_id AND proj_id = v_rec.proj_id AND pbl_id = v_rec.pbl_id AND seq_no = v_rec.seq_no and status_id = 'A'),(select loanable_amount from rf_pagibig_computation_itsreal where get_client_name_itsreal(entity_id,proj_server) = v_rec.entity_name and TRIM(proj_id) = TRIM(v_rec.proj_id) AND TRIM(pbl_id) = TRIM(v_rec.pbl_id) AND seq_no::INTEGER = v_rec.seq_no and TRIM(status_id) = 'A' and coalesce(server_id,'') = coalesce(v_rec.server_id) and coalesce(proj_server,'') = coalesce(v_rec.proj_server))::NUMERIC);
	END IF;
	*/
	
	IF v_tr_status.tran_date::date > '2022-07-04'::date then 
		c_col_prin := 
		(
			select sum(amount) from rf_client_ledger 
			where entity_id = v_rec.entity_id 
			and proj_id = v_rec.proj_id 
			and pbl_id = v_rec.pbl_id 
			and seq_no = v_rec.seq_no 
			and status_id = 'A' 
			and part_id in ('002','013')
			and coalesce(server_id, '') = COALESCE(v_rec.server_id, '') 
			and coalesce(proj_server, '') = coalesce(v_rec.proj_server, '')
			and date_paid::date BETWEEN '2022-07-04'::DATE and p_asof_date::DATE
		);
	ELSIF v_rec.server_id is nulL THEN
	    c_col_prin := 
		(
			select sum(amount) from rf_client_ledger 
			where entity_id = v_rec.entity_id 
			and proj_id = v_rec.proj_id 
			and pbl_id = v_rec.pbl_id 
			and seq_no = v_rec.seq_no 
			and status_id = 'A' 
			and part_id in ('002','013')
			and coalesce(server_id, '') = COALESCE(v_rec.server_id, '') 
			and coalesce(proj_server, '') = coalesce(v_rec.proj_server, '')
		);
	ELSE
		c_col_prin  := coalesce((c_NSP-v_balance),0);
	END IF;

	c_fulldp_date := v_dp_status.tran_date;
	c_turnover_date := coalesce(v_to_status.tran_date,v_assumed_to_status.tran_date);
	c_movein_date := v_movein_status.tran_date;
	c_docs_com_date := v_docs_com_status.actual_date;
	c_res_amt_tot := (select sum(amount) from rf_client_schedule where entity_id = v_rec.entity_id and proj_id = v_rec.proj_id and pbl_id = v_rec.pbl_id and seq_no = v_rec.seq_no and part_id = '012' and status_id = 'A' and coalesce(server_id, '') = COALESCE(v_rec.server_id, '') and coalesce(proj_server, '') = coalesce(v_rec.proj_server, '')); 
	--    v_days_past_due := sp_days_past_due(v_rec.entity_id, v_rec.proj_id, v_rec.pbl_id, v_rec.seq_no, p_asof_date, false);
	--    c_days_past_due := (case when v_days_past_due = 0 then null::numeric else v_days_past_due end);
	c_dp_amt_tot := (select sum(principal) from rf_client_schedule where entity_id = v_rec.entity_id and proj_id = v_rec.proj_id and pbl_id = v_rec.pbl_id and seq_no = v_rec.seq_no and part_id = '013' and status_id = 'A');
	c_proc_fee_tot := 
	(
		SELECT coalesce(sum(proc_fee), 0) + COALESCE(sum(rpt_amt), 0)
		from rf_client_schedule
		where entity_id = v_rec.entity_id
		and proj_id = v_rec.proj_id
		and pbl_id = v_rec.pbl_id
		and seq_no = v_rec.seq_no
		and part_id = '013'
		and status_id = 'A'
	);
	c_res_amt_paid := 
	(
		SELECT SUM(a.amount) 
		from rf_client_ledger a
		where entity_id = v_rec.entity_id 
		and proj_id = v_rec.proj_id 
		and pbl_id = v_rec.pbl_id 
		and seq_no = v_rec.seq_no 
		and part_id in ('012')
		and status_id = 'A'
		AND EXISTS 
		(
			SELECT *
			from rf_payments
			where pay_rec_id = a.pay_rec_id 
			and trans_date::date <= p_asof_date::date
		)
		and coalesce(server_id, '') = COALESCE(v_rec.server_id, '') and coalesce(proj_server, '') = coalesce(v_rec.proj_server, '')
	);
	c_proc_fee_paid := 
	(
		SELECT SUM(a.amount) 
		from rf_client_ledger a
		where entity_id = v_rec.entity_id 
		and proj_id = v_rec.proj_id 
		and pbl_id = v_rec.pbl_id 
		and seq_no = v_rec.seq_no 
		and part_id in ('019', '040')
		and status_id = 'A'
		AND EXISTS 
		(
			SELECT *
			from rf_payments
			where pay_rec_id = a.pay_rec_id 
			and trans_date::date <= p_asof_date::date
		)
	);
	
	c_dp_amt_paid := 
	(
		SELECT SUM(a.amount) 
		from rf_client_ledger a
		where entity_id = v_rec.entity_id 
		and proj_id = v_rec.proj_id 
		and pbl_id = v_rec.pbl_id 
		and seq_no = v_rec.seq_no 
		and part_id in ('013')
		and status_id = 'A'
		AND EXISTS 
		(
			SELECT *
			from rf_payments
			where pay_rec_id = a.pay_rec_id 
			and trans_date::date <= p_asof_date::date
		)
		and coalesce(server_id, '') = COALESCE(v_rec.server_id, '') and coalesce(proj_server, '') = coalesce(v_rec.proj_server, '')
	);
	
	c_amt_not_collected_due := 0;
	c_amt_not_collected_not_due := (COALESCE(c_res_amt_tot, 0) + coalesce(c_proc_fee_tot,0) + COALESCE(c_dp_amt_tot, 0) - coalesce(c_res_amt_paid, 0) - coalesce(c_proc_fee_paid, 0) - coalesce(c_dp_amt_paid,0));
	c_hse_model := (select model_alias from mf_product_model where model_id = v_rec.model_id and coalesce(server_id, '') = coalesce(v_rec.server_id, '') and coalesce(proj_server, '') = coalesce(v_rec.proj_server, ''));
	c_b_type := (select type_alias from mf_buyer_type where type_id = v_rec.buyertype);
	c_pay_stage := (select get_payment_stage_v2_with_date(v_rec.entity_id, v_rec.proj_id, v_rec.pbl_id, v_rec.seq_no, p_asof_date::DATE));
	
	/*
	c_pay_stage := ((v_mf_client_ledger_part.part_group) || 
	(select count(*) from rf_client_schedule rfs
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id 
	and pbl_id = v_rec.pbl_id 
	and seq_no = v_rec.seq_no 
	and status_id = 'A' 
	and coalesce(server_id,'') = coalesce(v_rec.server_id,'')
	and coalesce(proj_server,'') = coalesce(v_rec.proj_server,'')
	and (select trim(part_group) from mf_client_ledger_part where part_id = rfs.part_id) = trim(v_mf_client_ledger_part.part_group)
	and scheddate::date <= v_rf_client_ledger_latest.sched_date::date));

	IF TRIM(v_mf_client_ledger_part.part_group) = 'DP' THEN
	v_rf_client_ledger_dp_sum := sum(coalesce(amount,0)) from rf_client_ledger 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id 
	and pbl_id = v_rec.pbl_id 
	and seq_no = v_rec.seq_no 
	and status_id = 'A' 
	and coalesce(server_id,'') = coalesce(v_rec.server_id,'')
	and coalesce(proj_server,'') = coalesce(v_rec.proj_server,'')
	and sched_date::date = v_rf_client_ledger_latest.sched_date::date;

	IF v_rf_client_ledger_dp_sum >= v_rf_client_schedule.amount THEN

	select into v_rf_client_schedule_new_date * from rf_client_schedule 
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id 
	and pbl_id = v_rec.pbl_id 
	and seq_no = v_rec.seq_no 
	and status_id = 'A' 
	and coalesce(server_id,'') = coalesce(v_rec.server_id,'')
	and coalesce(proj_server,'') = coalesce(v_rec.proj_server,'')
	and date_part('month',scheddate::date) = (date_part('month',v_rf_client_ledger_latest.sched_date::date) + 1)
	and date_part('year',scheddate::date) = date_part('year',v_rf_client_ledger_latest.sched_date::date)
	order by scheddate desc;
	/*eto ung plus one sa pay stage para sa DP logic dito is pag full settled na ung bayad for that schedule ung pay stage na gagamitin mo na is ung susunod sa schedule*/	
	select into v_mf_client_ledger_part_new * from mf_client_ledger_part where trim(part_id) = trim(v_rf_client_schedule_new_date.part_id) and status_id = 'A';

	c_pay_stage 		:= ((v_mf_client_ledger_part_new.part_group) || 
	(select count(*) from rf_client_schedule rfs
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id 
	and pbl_id = v_rec.pbl_id 
	and seq_no = v_rec.seq_no 
	and status_id = 'A' 
	and coalesce(server_id,'') = coalesce(v_rec.server_id,'')
	and coalesce(proj_server,'') = coalesce(v_rec.proj_server,'')
	and (select trim(part_group) from mf_client_ledger_part where part_id = rfs.part_id) = trim(v_mf_client_ledger_part_new.part_group)
	and scheddate::date <= v_rf_client_schedule_new_date.scheddate::date));

	END IF;
	END IF;
	*/

	IF trim(c_b_type) similar to ('CASHS|CASHD') THEN
		SELECT into v_rf_client_schedule_res * 
		FROM rf_client_schedule 
		where entity_id = v_rec.entity_id 
		and proj_id = v_rec.proj_id 
		and pbl_id = v_rec.pbl_id 
		and seq_no = v_rec.seq_no 
		and part_id = '012'
		AND COALESCE(server_id, '') = COALESCE(v_rec.server_id, '') 
		and COALESCE(proj_server, '') = COALESCE(v_rec.proj_server, '')
		order by scheddate desc;

		select into v_rf_client_ledger_res * 
		FROM rf_client_ledger
		where entity_id = v_rec.entity_id 
		and proj_id = v_rec.proj_id 
		and pbl_id = v_rec.pbl_id 
		and seq_no = v_rec.seq_no 
		and part_id = '012'
		AND COALESCE(server_id, '') = COALESCE(v_rec.server_id, '') 
		and COALESCE(proj_server, '') = COALESCE(v_rec.proj_server, '')
		order by sched_date desc;

		IF (v_rf_client_ledger_res.sched_date::DATE >= v_rf_client_schedule_res.scheddate::DATE) THEN
			c_pay_stage := 'Full Res';
		END IF;

		IF v_fs_status.tran_date is not null THEN
			c_pay_stage	:= 'MA0';
		END IF;
	END IF;

	/*
	IF v_rec.server_id is null THEN
	c_pay_stage		:= (select pmt_stage from rf_card_pmt_status
	where entity_id = v_rec.entity_id 
	and proj_id = v_rec.proj_id 
	and pbl_id = v_rec.pbl_id 
	and seq_no = v_rec.seq_no);
	END IF;
	*/

	c_ntc_date := (select ntc from mf_unit_info where proj_id = v_rec.proj_id and pbl_id = v_rec.pbl_id and coalesce(server_id,'') = coalesce(v_rec.server_id,'') and coalesce(proj_server,'') = coalesce(v_rec.proj_server));

	SELECT INTO v_Co_NTP_Header * 
	from co_ntp_header a
	LEFT join co_ntp_detail b on b.ntp_no = a.ntp_no
	where b.pbl_id = v_rec.pbl_id
	and a.ntp_type_id = '02'
	and (case when a.status_id = 'I' then a.is_takeover_ntp ELSE a.status_id = 'A' END)
	AND a.entity_id != '7115114070'
	AND a.contract_no !~*'T2'
	order by a.date_created desc limit 1;

	v_NTP_No := v_Co_NTP_Header.ntp_no;

	IF v_Co_NTP_Header.contract_no ~*'SRH' and v_Co_NTP_Header.is_takeover_ntp = false and v_Co_NTP_Header.contract_no = (select contract_no from co_ntp_header a left join co_ntp_detail b on b.ntp_no = a.ntp_no where b.pbl_id = v_rec.pbl_id and a.ntp_type_id = '02' and (case when a.status_id = 'I' then a.is_takeover_ntp ELSE a.status_id = 'A' END) AND a.entity_id != '7115114070' order by a.date_created limit 1) THEN
		select into v_Original_Contract * from co_ntp_header where contract_no = REPLACE(v_Co_NTP_Header.contract_no, 'SR', '');

		v_Contract_No := v_Original_Contract.contract_no;
		v_NTP_No 	   := v_Original_Contract.ntp_no;
	END IF;

	select into v_Co_NTP_Detail * from co_ntp_detail where ntp_no = v_NTP_No and pbl_id = v_rec.pbl_id;

	select into v_Last_Accomplishment * from co_ntp_accomplishment where pbl_id = v_rec.pbl_id and ntp_no = v_NTP_No and status_id = 'A' ORDER BY as_of_date desc limit 1;

	--c_contract_no 	:= v_rec.contract_no ; COMMENTED BY LESTER 2019-01-08
	--c_contract_amt 	:= (case when v_rec.contract_amt = 0 then null::numeric else v_rec.contract_amt end);
	/*c_start_date 	:= v_rec.start_date;
	c_end_date 	:= v_rec.end_date ;
	c_perc_comp 	:= v_rec.perc_comp; commented by lester 2019-01-08*/

	c_contract_no := REPLACE(v_Co_NTP_Header.contract_no, 'SRH','H');
	c_contract_amt := (case when v_Co_NTP_Detail.other_cost = 0 then null::numeric else v_Co_NTP_Detail.other_cost end); 
	c_start_date := v_Co_NTP_Header.start_date;
	c_end_date := v_Co_NTP_Header.finish_date ;
	c_perc_comp := v_Last_Accomplishment.percent_accomplish;
	c_date_comp := (CASE WHEN v_Last_Accomplishment.percent_accomplish >= 100 THEN v_Last_Accomplishment.as_of_date else null end);
	v_amount_paid_percentage	:= (select ((c_col_prin/coalesce(c_NSP,1))*100)::numeric(19,2)); 
	RAISE INFO 'Principal: %', c_col_prin;
	RAISE INFO 'NSP: %', c_NSP;
	
	/*
	GROUPINGS
	'A. Not Yet Open for Sale'
	'B. Open for Sale'
	'C. Temporary Reserved'
	'D. OR - Unrealized'
	'E. Full DP (PagIBIG)'
	'F. Realized - Full DP (IHF)'
	'G.          - 25% Paid'
	'H.          - Full Settled'
	'I.          - Loan Filed'
	'J.          - Loan Released'
	'K.          - Full Settled (w/out Recourse AUB)'
	*/

	c_stage := null;
	/*
	if v_rec.mf_unit_info_status_id = 'A' then
	c_stage 		:= 'A. Not Yet Open for Sale';
	end if;
	*/

	if v_rec.mf_unit_info_status_id = 'A' then
		c_stage := 'B. Open for Sale';
	end if;

	if v_tr_status.tran_date is not null then
		c_stage := 'C. Temporary Reserved';
	end if;

	if v_or_status.tran_date is not null and v_realized_status.tran_date is null then
		c_stage := 'D. OR - Unrealized';
	end if;

	if v_dp_status.tran_date is not null and v_rec.group_id = '04' then
		c_stage := 'E. Full DP (PagIBIG)';
		c_realized_date := v_dp_status.tran_date;
	end if;
    
	RAISE INFO 'Amount percentage: %', v_amount_paid_percentage;
	RAISE INFO 'TR Date: %', v_tr_status.tran_date::date;
	
	IF v_tr_status.tran_date::date < '2022-07-04'::date and v_rec.server_id is NOT null THEN
	    
		if v_dp_status.tran_date is not null and v_rec.group_id = '02' then
			c_stage := 'F. Realized - Full DP (IHF)';
			c_realized_date := v_dp_status.tran_date;
		end if;
	ELSE
		if v_amount_paid_percentage >= 25 and v_dp_status.tran_date is not null and v_rec.group_id = '02' and v_fs_status.tran_date is null then
			c_stage := 'F. Realized - Full DP (IHF)';
			c_realized_date := v_dp_status.tran_date;
		end if;
	END IF;

	if v_amount_paid_percentage >= 25 and v_rec.group_id in ('02','03','04') and v_realized_status.tran_date is not null and v_rec.server_id is null then
		c_stage := 'G.          - 25% Paid';
		c_realized_date := v_realized_status.tran_date;
	end if;

	if v_fs_status.tran_date is not null then
		c_stage := 'H.          - Full Settled';
		c_realized_date := v_fs_status.tran_date;
	end if;

	if v_lf_status.tran_date is not null and v_rec.group_id IN ('04') then
		c_stage := 'I.          - Loan Filed';
		c_realized_date := v_lf_status.tran_date;
	end if;

	if (v_lr_status.tran_date is not null and v_rec.group_id IN ('04')) OR (v_fsmri_status.tran_date is not null and v_rec.group_id IN ('04')) then
		c_stage := 'J.          - Loan Released';
		c_realized_date := v_lr_status.tran_date;
	end if;

	if v_ofsaub_status.tran_date is not null then
		c_stage := 'K.          - Full Settled (w/out Recourse AUB)';
		c_realized_date := v_ofsaub_status.tran_date;
	end if;

	IF v_rec.rec_id IN (26248,26249,26250,26251,26252) THEN 
		c_stage := '';
	END IF;

	if c_stage is not null then
		RETURN NEXT;
	end if;

	END LOOP;

END;
$BODY$;

ALTER FUNCTION public.view_sched_trade_receivables_detailed_v4_debug(character varying, character varying, character varying, timestamp without time zone)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_sched_trade_receivables_detailed_v4_debug(character varying, character varying, character varying, timestamp without time zone) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_sched_trade_receivables_detailed_v4_debug(character varying, character varying, character varying, timestamp without time zone) TO employee;

GRANT EXECUTE ON FUNCTION public.view_sched_trade_receivables_detailed_v4_debug(character varying, character varying, character varying, timestamp without time zone) TO postgres;

