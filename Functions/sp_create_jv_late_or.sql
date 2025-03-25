-- PROCEDURE: public.sp_create_jv_late_or(character varying, date, character varying)

-- DROP PROCEDURE IF EXISTS public.sp_create_jv_late_or(character varying, date, character varying);

CREATE OR REPLACE PROCEDURE public.sp_create_jv_late_or(
	p_coid character varying,
	p_date date,
	p_user_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
declare
	
	--	Author: Emmanuel D. Apostol

	v_lateOR RECORD; 
	/*
	original asof Nov 9 2022
	v_entriesOR RECORD;
	*/ 
	/*
	modified by jari/lester asof Nov 9 2022
	*/ 
	v_entriesOR_debit RECORD; 
	v_entriesOR_credit RECORD; 

	v_intLine integer := 0; 
	v_strAcctID character varying; 
	
	v_jv_no character varying;  
	v_fiscal_year integer := (select date_part('year', p_date)::int::varchar); 
	v_period_id character varying := (select lpad(date_part('month', p_date)::int::varchar, 2, '0')); 
	
	v_tran_id character varying;  
	v_doc_id character varying; 
	v_proc_id integer; 
	v_forRev boolean := false; 
	v_rev_date date; 
	v_remarks character varying := 'TO RECORD THE ADJUSTMENTS FOR THE COLLECTION DATE ' || p_date || '; ' || chr(10)|| 'THE INCLUDED RECEIPT NO(s). ARE AS FOLLOWS:' || chr(10); 
	v_status_id character varying; 

	v_strPhase character varying; 
	v_strSubProjID character varying; 

	v_intCounter integer := 0; 
	v_payment_remarks character varying := ''; 

begin

	v_jv_no := fn_get_jv_no(v_fiscal_year, p_coid::varchar, v_period_id::int); 
	/*ADDED si_date by jari cruz asof jan 13 2023*/
	if 
	(
		select count(*) 
		from rf_payments x
		where coalesce(x.si_date,x.or_date) is not null 
		and (x.remarks ~* 'Late LTS/BOI' or x.remarks ~* 'Late OR Issuance for Good Check')
		and coalesce(x.si_date,x.or_date)::date = p_date and trim(x.status_id) = 'A' 
		and x.remarks !~* 'JV No'
		and x.co_id = p_coid
		--and x.pay_rec_id = 788475
	) > 0 then

		insert into rf_jv_header
		(
			co_id, busunit_id, jv_no, jv_date, fiscal_yr, period_id, tran_id, 
			posted_by, date_posted, doc_id, proc_id, is_for_reversal, reversal_date, remarks, 
			status_id, created_by, date_created, edited_by, date_edited
		)
		values 
		(
			p_coid, p_coid, v_jv_no, p_date, v_fiscal_year, v_period_id, '00007', 
			null, null, '11', 0::numeric, false, null, 
			'TO RECORD THE ADJUSTMENTS FOR THE COLLECTION DATE ' || p_date || '; ' || chr(10)|| 'THE INCLUDED RECEIPT NO(s). ARE AS FOLLOWS:' || chr(10), 
			'A', p_user_id, now(), null, null
		);

		--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
		--*				* *				* *				*
		--*	Insert OR Entries	* *	Insert OR Entries	* *	Insert OR Entries	*
		--*				* *				* *				*
		--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
		
		/*
		credit side
		modified by jari/lester asof Nov 9 2022
		Reason group by acct id filtered out same accounts but different payment side(debit or credit)
		*/
		FOR v_entriesOR_credit in
		(
			select a.acct_id, sum(a.trans_amt) as trans_amt, a.sub_proj_id, a.proj_id
			from
			(
				select a.acct_id, (a.trans_amt * -1) as trans_amt, b.*, 
				(
					select x.sub_proj_id 
					from mf_sub_project x
					where x.proj_id = b.proj_id 
					and x.phase = (select y.phase from mf_unit_info y where y.proj_id = b.proj_id and y.pbl_id = b.pbl_id and sub_proj_id = x.sub_proj_id)
				) as sub_proj_id
				from rf_crb_detail a
				inner join 
				(
					select coalesce(x.si_no,x.or_no) as or_no, x.pay_rec_id, coalesce(x.si_date,x.or_date), x.amount, x.client_seqno, 
					x.entity_id, x.proj_id, x.pbl_id, x.seq_no
					from rf_payments x
					where coalesce(x.si_date,x.or_date) is not null and coalesce(x.si_date,x.or_date)::date = p_date::date and trim(x.status_id) = 'A' 
					and (x.remarks ~* 'Late LTS/BOI' or x.remarks ~* 'Late OR Issuance for Good Check')
					and x.remarks !~* 'JV No.'
					and x.co_id = p_coid
				) b on b.or_no = a.rb_id and b.pay_rec_id::int = a.pay_rec_id::int
				where a.status_id = 'A'
				--and a.pay_rec_id::INT = 788475
				and a.trans_amt > 0
				--and a.doc_id = '307'
			) a
			group by a.acct_id, a.sub_proj_id, a.proj_id
			order by sum(a.trans_amt) desc, a.acct_id
		)
		loop

			v_intLine := v_intLine + 1;

			insert into rf_jv_detail 
			(
				co_id, busunit_id, jv_no, entry_no, line_no, acct_id, tran_amt, bal_side, ref_no, 
				project_id, sub_projectid, div_id, dept_id, sect_id, inter_co_id, inter_busunit_id, old_acct_id, 
				entity_id, pbl_id, seq_no, status_id, created_by, date_created, edited_by, date_edited
			)
			values
			(
				p_coid, p_coid, v_jv_no, 1, v_intLine, v_entriesOR_credit.acct_id, abs(coalesce(v_entriesOR_credit.trans_amt, 0)), 
				(
					case 
						when v_entriesOR_credit.trans_amt > 0
							then 'C'
						else 'D'
					end
				), 
				null, v_entriesOR_credit.proj_id, v_entriesOR_credit.sub_proj_id, null, null, null, null, null, null, 
				null, null, null, 'A', p_user_id, now(), null, null
			);

			RAISE INFO 'v_intLine: %', v_intLine;
			RAISE INFO 'Account: %', (select acct_name from mf_boi_chart_of_accounts where acct_id = v_entriesOR_credit.acct_id); 
			RAISE INFO 'Amount: %', v_entriesOR_credit.trans_amt;

		end loop;
		
		/*
		debit side
		modified by jari/lester asof Nov 9 2022
		Reason group by acct id filtered out same accounts but different payment side(debit or credit)
		*/
		FOR v_entriesOR_debit in
		(
			select a.acct_id, sum(a.trans_amt) as trans_amt, a.sub_proj_id, a.proj_id
			from
			(
				select a.acct_id, (a.trans_amt * -1) as trans_amt, b.*, 
				(
					select x.sub_proj_id 
					from mf_sub_project x
					where x.proj_id = b.proj_id 
					and x.phase = (select y.phase from mf_unit_info y where y.proj_id = b.proj_id and y.pbl_id = b.pbl_id and sub_proj_id = x.sub_proj_id)
				) as sub_proj_id
				from rf_crb_detail a
				inner join 
				(
					select coalesce(x.si_no,x.or_no) as or_no, x.pay_rec_id, coalesce(x.si_date,x.or_date), x.amount, x.client_seqno, 
					x.entity_id, x.proj_id, x.pbl_id, x.seq_no
					from rf_payments x
					where coalesce(x.si_date,x.or_date) is not null and coalesce(x.si_date,x.or_date)::date = p_date::date and trim(x.status_id) = 'A' 
					and (x.remarks ~* 'Late LTS/BOI' or x.remarks ~* 'Late OR Issuance for Good Check')
					and x.remarks !~* 'JV No.'
					and x.co_id = p_coid
				) b on b.or_no = a.rb_id and b.pay_rec_id::int = a.pay_rec_id::int
				where a.status_id = 'A'
				-- and a.pay_rec_id::INT = 788475
				-- and a.doc_id = '307'
				and a.trans_amt < 0
			) a
			group by a.acct_id, a.sub_proj_id, a.proj_id
			order by sum(a.trans_amt) desc, a.acct_id
		)
		loop

			v_intLine := v_intLine + 1;

			insert into rf_jv_detail 
			(
				co_id, busunit_id, jv_no, entry_no, line_no, acct_id, tran_amt, bal_side, ref_no, 
				project_id, sub_projectid, div_id, dept_id, sect_id, inter_co_id, inter_busunit_id, old_acct_id, 
				entity_id, pbl_id, seq_no, status_id, created_by, date_created, edited_by, date_edited
			)
			values
			(
				p_coid, p_coid, v_jv_no, 1, v_intLine, v_entriesOR_debit.acct_id, abs(coalesce(v_entriesOR_debit.trans_amt, 0)), 
				(
					case 
						when v_entriesOR_debit.trans_amt > 0
							then 'C'
						else 'D'
					end
				), 
				null, v_entriesOR_debit.proj_id, v_entriesOR_debit.sub_proj_id, null, null, null, null, null, null, 
				null, null, null, 'A', p_user_id, now(), null, null
			);

			RAISE INFO 'v_intLine: %', v_intLine;
			RAISE INFO 'Account: %', (select acct_name from mf_boi_chart_of_accounts where acct_id = v_entriesOR_debit.acct_id); 
			RAISE INFO 'Amount: %', v_entriesOR_debit.trans_amt;

		end loop;

		--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
		--*				* *				* *				*
		--*	  Update Payments	* *	  Update Payments	* *	  Update Payments	*
		--*				* *				* *				*
		--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
		for v_lateOR in 
		(
			select coalesce(x.si_no,x.or_no) as or_no, x.pay_rec_id, coalesce(x.si_date,x.or_date), x.amount, x.client_seqno, 
			x.entity_id, x.proj_id, x.pbl_id, x.seq_no
			from rf_payments x
			where coalesce(x.si_date,x.or_date) is not null 
			and (x.remarks ~* 'Late LTS/BOI' or x.remarks ~* 'Late OR Issuance for Good Check')
			and coalesce(x.si_date,x.or_date)::date = p_date and trim(x.status_id) = 'A' 
			and x.remarks !~* 'JV No'
			and x.co_id = p_coid
			--and x.pay_rec_id = 788475
			order by coalesce(x.si_date,x.or_date)
		)
		loop

			if v_intCounter < 4::int then
				v_remarks := v_remarks || v_lateOR.or_no || '(' || v_lateOR.amount || '), '; 
				v_intCounter := v_intCounter + 1;
			else
				v_remarks := v_remarks || v_lateOR.or_no || '(' || v_lateOR.amount || '), ' || chr(10); 
				v_intCounter := 0;
			end if; 

			v_payment_remarks := 
			(
				select 
				(
					case 
						when length(coalesce(remarks, '')) > 0 
							then trim(coalesce(remarks, '')) || (case when right(trim(coalesce(remarks, '')), '1') = ';' then '' else '; ' end)
						else '' 
					end
				) || ' JV No. ' || v_jv_no || '; ' 
				from rf_payments 
				where coalesce(si_no,or_no) = v_lateOR.or_no and pay_rec_id::int = v_lateOR.pay_rec_id::int
			); 

			RAISE INFO 'Payment Remarks: %', v_payment_remarks; 

			update rf_payments 
			set remarks = v_payment_remarks
			where coalesce(si_no,or_no) = v_lateOR.or_no and pay_rec_id::int = v_lateOR.pay_rec_id::int; 

		end loop; 

		v_remarks := left(trim(v_remarks), length(trim(v_remarks))::int - 1::int); 
		RAISE INFO 'v_remarks: %', v_remarks; 

		update rf_jv_header
		set remarks = v_remarks
		where jv_no = v_jv_no
		and co_id = p_coid; --ADDED COMPANY BY LESTER 2021-09-07 TO AVOID OVERWRITING OF REMARKS

	else 
		RAISE INFO 'This date has already been subjected to JV creation.'; 
	end if; 

end;
$BODY$;

ALTER PROCEDURE public.sp_create_jv_late_or(character varying, date, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON PROCEDURE public.sp_create_jv_late_or(character varying, date, character varying) TO PUBLIC;

GRANT EXECUTE ON PROCEDURE public.sp_create_jv_late_or(character varying, date, character varying) TO admin;

GRANT EXECUTE ON PROCEDURE public.sp_create_jv_late_or(character varying, date, character varying) TO employee;

GRANT EXECUTE ON PROCEDURE public.sp_create_jv_late_or(character varying, date, character varying) TO postgres;

