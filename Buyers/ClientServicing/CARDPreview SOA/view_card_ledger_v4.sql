-- FUNCTION: public.view_card_ledger_v4(character varying, character varying, character varying, integer, boolean)

-- DROP FUNCTION IF EXISTS public.view_card_ledger_v4(character varying, character varying, character varying, integer, boolean);

CREATE OR REPLACE FUNCTION public.view_card_ledger_v4(
	p_entity_id character varying,
	p_proj_id character varying,
	p_pbl_id character varying,
	p_seq_no integer,
	p_refund boolean)
    RETURNS TABLE(c_actual_date timestamp without time zone, c_trans_date timestamp without time zone, c_sched_date timestamp without time zone, c_amount_paid numeric, c_pico numeric, c_proc_fees numeric, c_rpt_amt numeric, c_res numeric, c_dp numeric, c_mri numeric, c_fire numeric, c_vat numeric, c_soi numeric, c_sop numeric, c_penalty numeric, c_cbp numeric, c_adjustment numeric, c_interest numeric, c_principal numeric, c_balance numeric, c_percent_paid numeric, c_pay_rec_id integer, c_due_type character varying, c_receipt_no character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

	v_Pay_Part_ID VARCHAR;
	v_intRecID INTEGER;
	v_runningReservation NUMERIC;
	v_runningDownpayment NUMERIC;
	v_runningPrincipal NUMERIC;

	v_totalReservation NUMERIC;
	v_totalDownpayment NUMERIC;
	v_totalPrincipal NUMERIC;
	v_totalProcFee NUMERIC;

	v_numNSP NUMERIC;
	v_numBalance NUMERIC;
	v_tmpSchedDate TIMESTAMP;
	v_intRow INTEGER;

	v_recSchedule RECORD;
	v_recLedger RECORD;
	v_Receipt_ID VARCHAR;

	v_Principal_Sched NUMERIC;
	v_Principal_Sched_Paid NUMERIC;
	v_Proc_Fee_Sched NUMERIC;

	v_check_no VARCHAR;
	v_minORdate TIMESTAMP;
	v_actualDate TIMESTAMP;
	v_Payment RECORD;
	v_sold_unit RECORD;
	v_TR_DATE DATE;
	
	--LAST MODIFIED BY LESTER 2023-02-27

BEGIN
    select into v_sold_unit * from rf_sold_unit  WHERE entity_id = p_entity_id AND projcode = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no;
	v_TR_DATE          := (SELECT actual_date::DATE FROM rf_buyer_status WHERE TRIM(entity_id) = p_entity_id AND TRIM(proj_id) = p_proj_id AND TRIM(pbl_id) = p_pbl_id AND seq_no = p_seq_no AND TRIM(byrstatus_id) = '17' AND status_id = 'A');

	v_numNSP			:= (SELECT net_sprice FROM rf_client_price_history WHERE entity_id = p_entity_id AND proj_ID = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND status_id = 'A');
	v_totalReservation	:= (SELECT SUM(principal) FROM rf_client_schedule WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '012' AND status_id = 'A');
	
-- 	IF p_entity_id = '3436559580' then
-- 		v_totalDownpayment := (SELECT SUM(principal) FROM rf_client_schedule WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '013' AND status_id = 'A');
-- 	ELSE
		v_totalDownpayment	:= v_numNSP; 
	--END IF;
	
	v_totalPrincipal	:= (SELECT SUM(principal) FROM rf_client_schedule WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '014' AND status_id = 'A');
	v_totalProcFee		:= (SELECT SUM(proc_fee) from rf_client_schedule where entity_id = p_entity_id and proj_id = p_proj_id and pbl_id = p_pbl_id and seq_no = p_seq_no and part_id = '013' and status_id = 'A');
	v_minORdate			:= (SELECT min(or_date) from rf_payments where entity_id = p_entity_id and pbl_id = p_pbl_id and seq_no = p_seq_no and or_date is not null and status_id != 'I');

	
	IF v_sold_unit.server_id is NOT null AND v_TR_DATE::DATE < '2022-07-04'::DATE and p_entity_id != '3436559580'  THEN
		
-- 		IF p_entity_id = '6915008026' AND p_proj_id = '007' and p_pbl_id = '32' and p_seq_no = 1 then
-- 		v_totalPrincipal :=	(select amount + balance from rf_client_ledger a
-- 							WHERE a.entity_id = p_entity_id AND a.proj_id = p_proj_id AND a.pbl_id = p_pbl_id AND a.seq_no = p_seq_no AND a.part_id IN ('002', '013') AND a.status_id = 'A' 
-- 							and not exists (select *
-- 											from rf_payments
-- 											where entity_id = a.entity_id 
-- 											and proj_id = a.proj_id 
-- 											and pbl_id = a.pbl_id 
-- 											and seq_no = a.seq_no 
-- 											and pay_rec_id = a.pay_rec_id
-- 											and trim(status_id) = 'A')
-- 							order by a.sched_date desc, a.client_ledger_id desc limit 1);
		
		IF p_entity_id = '2102086039' AND p_proj_id = '003' and p_pbl_id = '7132' and p_seq_no = 2 then
			v_totalPrincipal :=	(select balance from rf_client_ledger a
							WHERE a.entity_id = p_entity_id AND a.proj_id = p_proj_id AND a.pbl_id = p_pbl_id AND a.seq_no = p_seq_no AND a.part_id IN ('002', '013') AND a.status_id = 'A' 
							and not exists (select *
											from rf_payments
											where entity_id = a.entity_id 
											and proj_id = a.proj_id 
											and pbl_id = a.pbl_id 
											and seq_no = a.seq_no 
											and pay_rec_id = a.pay_rec_id
											and status_id = 'A')
							order by a.sched_date desc, a.client_ledger_id desc limit 1);
		ELSIF p_entity_id = '7038968092' AND p_proj_id = '001' and p_pbl_id = '5412' and p_seq_no = 3 then
			v_totalPrincipal :=	(select balance from rf_client_ledger a
							WHERE a.entity_id = p_entity_id AND a.proj_id = p_proj_id AND a.pbl_id = p_pbl_id AND a.seq_no = p_seq_no AND a.part_id IN ('002', '013') AND a.status_id = 'A' 
							and not exists (select *
											from rf_payments
											where entity_id = a.entity_id 
											and proj_id = a.proj_id 
											and pbl_id = a.pbl_id 
											and seq_no = a.seq_no 
											and pay_rec_id = a.pay_rec_id
											and status_id = 'A')
							order by a.sched_date desc, a.client_ledger_id desc limit 1);
		ELSIF p_entity_id = '7345358424' AND p_proj_id = '019' and p_pbl_id = '1051' and p_seq_no = 3 then				
			v_totalPrincipal :=	(select balance from rf_client_ledger a
							WHERE a.entity_id = p_entity_id AND a.proj_id = p_proj_id AND a.pbl_id = p_pbl_id AND a.seq_no = p_seq_no AND a.part_id IN ('002', '013') AND a.status_id = 'A' 
							and not exists (select *
											from rf_payments
											where entity_id = a.entity_id 
											and proj_id = a.proj_id 
											and pbl_id = a.pbl_id 
											and seq_no = a.seq_no 
											and pay_rec_id = a.pay_rec_id
											and status_id = 'A')
							order by a.sched_date desc, a.client_ledger_id desc limit 1);				
							
		else
			v_totalPrincipal :=	(select amount + balance from rf_client_ledger a
							WHERE a.entity_id = p_entity_id AND a.proj_id = p_proj_id AND a.pbl_id = p_pbl_id AND a.seq_no = p_seq_no AND a.part_id IN ('002', '013') AND a.status_id = 'A'
							and exists (select * from rf_payments where entity_id = a.entity_id and proj_id = a.proj_id and pbl_id = a.pbl_id and seq_no = a.seq_no and pay_rec_id = a.pay_rec_id and trim(status_id) = 'A')	 
							order by sched_date limit 1);
		end if;
		
		IF p_entity_id = '7429912528' and p_proj_id = '001' and p_pbl_id = '3430' and p_seq_no = 4 then
			v_totalPrincipal := 1322700;
		end if;
		RAISE INFO 'Total Principal sa Itsreal: %', v_totalPrincipal;
	ELSE
	    IF get_group_id(v_sold_unit.buyertype) = '03' THEN
			v_totalPrincipal   := (SELECT SUM(principal) FROM rf_client_schedule WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '013' AND status_id = 'A');
		ELSE
			v_totalPrincipal   := (SELECT SUM(principal) FROM rf_client_schedule WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '014' AND status_id = 'A');
		END IF;
	END IF;

	FOR v_recLedger IN 
	(
		/*
		SELECT row_number() OVER(PARTITION BY a.pay_rec_id ORDER BY a.appl_date, a.pay_rec_id, (case when a.due_type = 'M' then 0 when a.due_type = 'W' then 1 else 2 end), 
		COALESCE(a.sched_date, b.date_paid)) as row_number, 

		b.date_paid, 

		a.appl_date, a.sched_date, a.pay_rec_id, coalesce(a.due_type, '') as due_type
		FROM rf_client_ledger a
		inner join 
		(
			select x.entity_id, x.proj_id, x.pbl_id, x.seq_no, x.pay_rec_id, x.trans_date, x.or_date, x.or_doc_id, 
			(case when x.or_doc_id is not null then x.or_date else x.trans_date end) as date_paid
			from rf_payments x
			group by x.entity_id, x.proj_id, x.pbl_id, x.seq_no, x.pay_rec_id, x.trans_date, x.or_date, x.or_doc_id
			order by x.trans_date, x.or_date
		) b on a.pay_rec_id::int = b.pay_rec_id::int
		WHERE a.entity_id = p_entity_id AND a.proj_id = p_proj_id AND a.pbl_id = p_pbl_id AND a.seq_no = p_seq_no
		AND a.status_id = 'A' AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(a.request_no), '') IS NULL) END)
		GROUP BY a.appl_date, a.pay_rec_id, a.sched_date, b.date_paid, a.due_type
		ORDER BY a.appl_date, a.pay_rec_id, 
		(case when a.due_type = 'M' then 0 when a.due_type = 'W' then 1 else 2 end), 
		COALESCE(a.sched_date, b.date_paid)
		*/
		
		select *
		from
		(
			select *
			from
			(
				SELECT row_number() OVER(PARTITION BY a.pay_rec_id ORDER BY a.appl_date, a.pay_rec_id, (case when a.due_type = 'M' then 0 when a.due_type = 'W' then 1 else 2 end), 
				COALESCE(a.sched_date, b.date_paid)) as row_number, b.date_paid, get_ledger_apply_date(a.pay_rec_id) as appl_date, a.sched_date, a.pay_rec_id, coalesce(a.due_type, '') as due_type, 

				b.receipt_id, b.check_no, b.pay_part_id, b.or_no

				FROM rf_client_ledger a
				LEFT join 
				(
					select x.entity_id, x.proj_id, x.pbl_id, x.seq_no, x.pay_rec_id, x.trans_date, x.amount, coalesce(x.or_date, x.si_date), coalesce(x.or_doc_id, x.si_doc_id), 
					(case when coalesce(x.or_doc_id, x.si_doc_id) is not null
					 and x.entity_id !=  '3436559580'
					 then coalesce(x.or_date, x.si_date) 
					 
					 else x.trans_date end) as date_paid,
					x.receipt_id, x.check_no, x.pay_part_id, coalesce(x.or_no, x.si_no) as or_no
					from rf_payments x
					where (case when x.pay_part_id in ('033', '262', '263') THEN coalesce(x.or_doc_id, x.si_doc_id) is not null else true end)
					
					group by x.entity_id, x.proj_id, x.pbl_id, x.seq_no, x.pay_rec_id, x.trans_date, x.amount, coalesce(x.or_date, x.si_date), coalesce(x.or_doc_id, x.si_doc_id), 
					x.receipt_id, x.check_no, x.pay_part_id, coalesce(x.or_no, x.si_no)
					order by x.trans_date, coalesce(x.or_date, x.si_date)
				) b on a.pay_rec_id::int = b.pay_rec_id::int
				WHERE a.entity_id = p_entity_id AND a.proj_id = p_proj_id AND a.pbl_id = p_pbl_id AND a.seq_no = p_seq_no
				--and a.pay_rec_id = 785835
				AND a.status_id = 'A'
				
				AND (CASE WHEN false THEN TRUE ELSE (NULLIF(TRIM(a.request_no), '') IS NULL) END)
				GROUP BY a.appl_date, a.pay_rec_id, a.sched_date, b.date_paid, a.due_type, 
				b.receipt_id, b.check_no, b.pay_part_id, b.or_no
				ORDER BY get_ledger_apply_date(p_entity_id, p_proj_id,p_pbl_id, p_seq_no, a.pay_rec_id), a.pay_rec_id, a.sched_date ,a.pay_rec_id
			) a
			union
			select 1::bigint, COALESCE(a.si_date, a.or_date), a.trans_date, null::timestamp as sched_date, a.pay_rec_id, ''::varchar as due_type, 
			a.receipt_id, a.check_no, a.pay_part_id, COALESCE(a.si_no, a.or_no)
			from rf_payments a
			where a.entity_id = p_entity_id AND a.proj_id = p_proj_id AND a.pbl_id = p_pbl_id AND a.seq_no = p_seq_no
			AND trim(a.status_id) = 'A' and a.pay_part_id = '087'
		) a
		ORDER BY a.appl_date, a.pay_rec_id, (case when a.due_type = 'M' then 0 when a.due_type = 'W' then 1 else 2 end), COALESCE(a.sched_date, a.date_paid)
	)
	LOOP
		SELECT INTO v_Payment * FROM rf_payments where entity_id = p_entity_id and proj_id = p_proj_id and pbl_id = p_pbl_id and seq_no = p_seq_no and pay_rec_id = v_recLedger.pay_rec_id;
		
		v_intRow		:= v_recLedger.row_number;
		v_intRecID		:= v_recLedger.pay_rec_id;
		c_pay_rec_id	:= v_recLedger.pay_rec_id;
		c_due_type		:= v_recLedger.due_type; 

		v_Receipt_ID	:= v_recLedger.receipt_id; 
		v_check_no		:= v_recLedger.check_no; 
		c_actual_date	:= v_recLedger.date_paid;
		c_trans_date	:= v_recLedger.appl_date;
		c_sched_date	:= v_recLedger.sched_date;
		v_Pay_Part_ID	:= v_recLedger.pay_part_id; 
		
		v_Principal_Sched      := (select SUM(COALESCE(principal, 0)) FROM rf_client_schedule where entity_id = p_entity_id and proj_id = p_proj_id and pbl_id = p_pbl_id and seq_no = p_seq_no and scheddate::dATE = c_sched_date and status_id = 'A');

		v_Principal_Sched_Paid := (SELECT SUM(amount) from rf_client_ledger where entity_id = p_entity_id and proj_id = p_proj_id and pbl_id = p_pbl_id and seq_no = p_seq_no and sched_date = c_sched_date AND status_id = 'A');
		
		IF v_intRecID = 94113 THEN
			RAISE INFO 'Row Number: %', v_intRow;
		END IF;
		
		IF v_intRow = 1 THEN

		    if exists (select * from mf_pay_particular where pay_part_id = v_recLedger.pay_part_id and apply_ledger and status_id = 'A') and exists (SELECT * FROM rf_payments where entity_id = p_entity_id and proj_id = p_proj_id and pbl_id = p_pbl_id and seq_no = p_seq_no and pay_rec_id = v_intRecID) THEN
			
			IF v_intRecID = 94113 THEN
				RAISE INFO 'Trans Date: %', c_trans_date;
			END IF;
			
			c_amount_paid := 
			(
				SELECT SUM(amount)
				FROM rf_client_ledger
				WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND status_id = 'A'
				AND /*appl_date = c_trans_date AND*/ pay_rec_id = v_intRecID
				AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
			);
	             ELSE
			c_amount_paid := (select amount from rf_payments where trim(entity_id) = p_entity_id and trim(proj_id) = p_proj_id and trim(pbl_id) = p_pbl_id and seq_no = p_seq_no and pay_rec_id = v_intRecID and status_id = 'A');
	             END IF;
		ELSE
			c_amount_paid := NULL;
			
			
		END IF;

		if nullif(NULLIF(v_Receipt_ID, 'MBTC'), '') is not null and v_Receipt_ID != 'AUB' and v_Payment.server_id IS NULL and v_Payment.si_date IS NULL AND v_Payment.or_date is null then
			c_actual_date := 
			(
				select a.trans_date
				from
				(
					select x.trans_date, x.entity_id, y.check_no, x.total_amt_paid::numeric(19, 2) as amount
					from rf_tra_header x
					inner join rf_tra_detail y on x.client_seqno = y.client_seqno and x.receipt_no = y.receipt_no
					where x.receipt_no = v_Receipt_ID
					union
					select x.trans_date, x.entity_id, x.check_no, x.amount::numeric(19, 2) as amount
					from rf_payments x
					where x.or_no = v_Receipt_ID
				) a 
				where (a.entity_id = p_entity_id 
				or p_entity_id = '5826363777' or p_entity_id = '4812947879' or p_entity_id = '8519844496'
				or p_entity_id = '0001101214' or p_entity_id = '5647250544' or p_entity_id = '5575474891'
				or p_entity_id = '0154686137' or p_entity_id = '0269069676' or p_entity_id = '8731161872'
				or p_entity_id = '9556709867' or p_entity_id = '0655653705' or p_entity_id = '6666531953'
				or p_entity_id = '7943417252' or p_entity_id = '7785069900' or p_entity_id = '1704742852'
				or p_entity_id = '0832508053' or p_entity_id = '1286273030' or p_entity_id = '9569019269'
				or p_entity_id = '5054606935' or p_entity_id = '5365104763' or p_entity_id = '8772233044'
				or p_entity_id = '6856986526' or p_entity_id = '7650494090' or p_entity_id = '4007896049'
				or p_entity_id = '9795641903' or p_entity_id = '9229297218' or p_entity_id = '2444008258'
				or p_entity_id = '7585281440' or p_entity_id = '9029976310' OR p_entity_id = '3199527957'
				or p_entity_id = '7254382921' OR p_entity_id = '9029976310' OR p_entity_id = '7350733978'
				OR p_entity_id = '8551929853' or p_entity_id = '1618525153' or p_entity_id = '7191798048'
				or p_entity_id = '4492983068' or p_entity_id = '0757445265' or p_entity_id = '5439287112'
				or p_entity_id = '4973777406' or p_entity_id = '1460057851' or p_entity_id = '0926622511'
				OR p_entity_id = '9744271998' or p_entity_id = '0585295386' or p_entity_id = '4519940125'
				OR p_entity_id = '1990398429' or p_entity_id = '0374171329' or p_entity_id = '2405893574'
				OR p_entity_id = '0115874535' or p_entity_id = '0003664820' or p_entity_id = '8652996616'
				or p_entity_id = '1950689261' or p_entity_id = '1178288679' or p_entity_id = '0842990563'
				or p_entity_id = '5321132317' or p_entity_id = '1719120042' or p_entity_id = '6523048644'
				OR p_entity_id = '2513755723'
				)
				and (a.check_no = v_check_no or a.check_no is null)
				and 
				(
					a.amount::numeric(19, 2) 
					= 
					(
						SELECT SUM(amount) 
						FROM rf_client_ledger
						WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no 
						AND status_id = 'A' AND appl_date = c_trans_date AND pay_rec_id = v_intRecID
						AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
					)::numeric(19, 2)
					or
					p_entity_id = '7936173733'
				)
				limit 1
			);
		end if;
		
		--c_actual_date := v_Payment.or_date;

		IF v_intRecID = 28077 then
			c_actual_date := '2018-07-26 11:50:29.541746';
		end if;

		--Processing Fee

		c_proc_fees := 
		(
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no 
			AND part_id IN ('019', '038', '040') AND status_id = 'A' AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
			and coalesce(due_type, '') = c_due_type
		);

		c_rpt_amt :=
		(
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no 
			AND part_id IN ('040') AND status_id = 'A' AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
			and coalesce(due_type, '') = c_due_type
		);

		--Reservation
		c_res := 
		(
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '012' AND status_id = 'A'
			AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
		);

		--Downpayment

		c_dp := 
		(
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no 
			AND part_id = '013' AND status_id = 'A' AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
			and coalesce(due_type, '') = c_due_type
		);

		c_mri := 
		(
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '003' AND status_id = 'A'
			AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
			and coalesce(due_type, '') = c_due_type
		);

		--Fire
		 c_fire := 
		 (
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '004' AND status_id = 'A'
			AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
		 );

		--VAT
		c_vat := 
		(
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id in ('023','008') AND status_id = 'A'
			AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
			and coalesce(due_type, '') = c_due_type
		);
		
		IF v_Payment.pay_part_id = '087' THEN --DCRF 1485
				c_vat := 
					(
						SELECT vat
						FROM rf_pagibig_lnrel
						WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no 
					);
					
				--c_amount_paid := c_amount_paid + COALESCE(c_vat, 0);
		END IF;

		--SOI
		c_soi := 
		(
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '006' AND status_id = 'A'
			AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
		);

		--SOP
		c_sop := 
		(
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '007' AND status_id = 'A'
			AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
		);

		--PENALTY IN DOWNPAYMENT 
		c_penalty := 
		(
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '020' AND status_id = 'A'
			AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
			and coalesce(due_type, '') = c_due_type
		);

		--Interest
		c_interest := 
		(
			SELECT SUM(amount)
			FROM rf_client_ledger
			WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '001' AND status_id = 'A'
			AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
			AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
		);

		--Principal
		--if (v_Pay_Part_ID != '087' and v_Pay_Part_ID is not null) then
			c_principal := 
			(
				SELECT SUM(amount)
				FROM rf_client_ledger
				WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id in ('002', '039') AND status_id = 'A'
				AND sched_date = c_sched_date AND pay_rec_id = v_intRecID
				AND (CASE WHEN p_refund THEN TRUE ELSE (NULLIF(TRIM(request_no), '') IS NULL) END)
			);
			
			IF v_Payment.pay_part_id = '087' THEN --DCRF 1485
				c_principal := 
					(
						SELECT amount
						FROM rf_payments
						WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no 
						AND pay_part_id = '087' AND pay_rec_id = v_intRecID AND status_id = 'A'
					);
					
				c_principal := c_amount_paid - c_vat;
			END IF;
		/*else
			c_principal := 
			(
				SELECT amount
				FROM rf_payments
				WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no 
				AND pay_part_id = '087' AND pay_rec_id = v_intRecID AND status_id = 'A'
			);
		end if;*/

		v_runningPrincipal := (COALESCE(v_runningPrincipal, 0) + COALESCE(c_dp, 0) + COALESCE(c_principal, 0));
		
		c_percent_paid := NULLIF((v_runningPrincipal / v_numNSP) * 100, 0);
		
		IF c_percent_paid IS NULL THEN
			c_percent_paid := 0;
		END IF;
		
-- 		RAISE INFO 'Running Principal: %', v_runningPrincipal;
-- 		RAISE INFO 'NSP: %', v_numNSP;
		
		--RAISE INFO 'v_numNSP: %', v_numNSP; 
		--RAISE INFO 'v_Pay_Part_ID: %', v_Pay_Part_ID; 
		
		IF c_res IS NOT NULL THEN
			v_totalReservation	:= (v_totalReservation - c_res);
			c_balance			:= v_totalDownpayment;
			
			RAISE INFO 'balance sa reservation: %s', c_balance;
		END IF;
		
		IF c_dp IS NOT NULL THEN
-- 			v_totalDownpayment	:= (v_totalDownpayment - c_dp);
-- 			c_balance			:= v_totalDownpayment;

			IF p_entity_id = '7429912528' and p_proj_id = '001' and p_pbl_id = '3430' and p_seq_no = 4 THEN
				v_totalDownpayment := (v_totalDownpayment - c_dp);
				c_balance := v_totalDownpayment;
			ELSE
				IF v_sold_unit.server_id IS NOT NULL and p_entity_id != '3436559580' THEN
					v_totalPrincipal := (v_totalPrincipal - COALESCE(c_dp, 0));
					c_balance := v_totalPrincipal;
				ELSE
					v_totalDownpayment := (v_totalDownpayment - c_dp);
					c_balance := v_totalDownpayment;
				END IF;
			
			END IF;
			
			
		END IF;

		IF c_principal IS NOT NULL THEN
-- 			v_totalPrincipal	:= (v_totalPrincipal - c_principal);
-- 			c_balance			:= v_totalPrincipal;
			IF v_sold_unit.server_id IS NOT NULL THEN
				RAISE INFO '*******************';
				raise info 'Total Principal: %', v_totalPrincipal;
				RAISE INFO 'Principal: %', c_principal;
				RAISE INFO '*******************';
				v_totalPrincipal := (v_totalPrincipal - c_principal);
				c_balance := v_totalPrincipal;
			ELSE
				v_totalPrincipal := (v_totalPrincipal - c_principal);
				c_balance := v_totalPrincipal;
			end if;
			
		ELSif NULLIF(c_principal, 0) IS NOT NULL AND c_dp IS NULL then
		    c_balance := v_totalPrincipal;
			RAISE INFO 'Value of Total Principal: %', v_totalPrincipal;
			RAISE INFO 'Dumaan dito sa else';
			
		ELSIF NULLIF(c_principal, 0) IS NULL AND v_Principal_Sched IS NOT NULL and COALESCE(c_dp, 0) = 0 and COALESCE(c_res, 0) = 0 THEN
			c_balance := v_totalPrincipal;
		END IF;
		
		--RAISE INFO 'balance sa DP: %', c_balance;
		
		IF v_sold_unit.server_id IS NOT NULL THEN
			c_percent_paid := NULLIF(((v_numNSP-c_balance) / v_numNSP) * 100, 0);
		END IF;

		if (select remarks from rf_payments where entity_id = p_entity_id and proj_id = p_proj_id and pbl_id = p_pbl_id and seq_no = p_seq_no and pay_rec_id = v_intRecID and status_id = 'A') ~*'Payment from MBTC' THEN
			c_trans_date := (SELECT trans_date from rf_payments where entity_id = p_entity_id and proj_id = p_proj_id and pbl_id = p_pbl_id and seq_no = p_seq_no and pay_rec_id = v_intRecID and status_id = 'A');
		END IF;
		
		c_receipt_no	:=	v_recLedger.or_no;
		
		IF v_intRecID = 562773 THEN
			c_receipt_no := '000458';
		END IF;
		
		IF EXISTS (SELECT * FROM rf_payments where entity_id = p_entity_id and proj_id = p_proj_id and pbl_id = p_pbl_id and seq_no = p_seq_no and pay_rec_id = v_intRecID
				   and v_intRecID not in (785811, 785812, 785813)) then
			RETURN NEXT;
		end if;
	
		
		--RETURN NEXT;
	END LOOP;

RETURN;

END;
$BODY$;

ALTER FUNCTION public.view_card_ledger_v4(character varying, character varying, character varying, integer, boolean)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_card_ledger_v4(character varying, character varying, character varying, integer, boolean) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_card_ledger_v4(character varying, character varying, character varying, integer, boolean) TO employee;

GRANT EXECUTE ON FUNCTION public.view_card_ledger_v4(character varying, character varying, character varying, integer, boolean) TO postgres;

