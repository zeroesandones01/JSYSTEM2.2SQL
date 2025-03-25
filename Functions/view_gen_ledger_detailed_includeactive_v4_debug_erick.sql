-- FUNCTION: public.view_gen_ledger_detailed_includeactive_v4_debug_erick(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer, integer)

-- DROP FUNCTION IF EXISTS public.view_gen_ledger_detailed_includeactive_v4_debug_erick(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer, integer);

CREATE OR REPLACE FUNCTION public.view_gen_ledger_detailed_includeactive_v4_debug_erick(
	p_acct_id character varying,
	p_co_id character varying,
	p_date_fr character varying,
	p_date_to character varying,
	p_proj_id character varying,
	p_phase_id character varying,
	p_status_id character varying,
	p_include_month character varying,
	p_period_fr integer,
	p_period_to integer)
    RETURNS TABLE(c_t_date character varying, c_description character varying, c_div character varying, c_dept character varying, c_project_id character varying, c_sub_projectid character varying, c_debit numeric, c_credit numeric, c_run_bal numeric, c_jv_no character varying, c_cv_no character varying, c_pv_no character varying, c_or_no character varying, c_ar_no character varying, c_pfr_no character varying, c_si_no character varying, c_remarks character varying, c_status character varying, c_payee character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

--ORIGINAL FUNCTION IS view_gen_ledger_detailed_includeactive_v2
v_isYearClosed BOOLEAN;
v_recGL RECORD;

BEGIN

	/**used to check if year is already closed - once closed, ending bal of previous year will be carried over to beg bal of next year*/
	v_isYearClosed := 
	(
		select (case when jv_no is not null then true else false end) 
		from rf_jv_header where tran_id = '00030' 
		and fiscal_yr::text = (to_char(p_date_fr::date,'yyyy')::int - 1)::text
		and co_id = p_co_id and status_id = 'P'
	);
	
	RAISE INFO 'Year Closed: %',  v_isYearClosed;

	FOR v_recGL IN 
	(
		-------------------------------BEGIN GENERAL LEDGER SQL

		select T_date, description, div_id, dept_id, project_id, sub_projectid, debit, credit, sum(debit-credit) 
		over (order by Gl_date, description, div_id, dept_id, project_id, sub_projectid, debit, credit,jv_no,  cv_no, pv_no, or_no, ar_no, pfr_no, remarks,  status) as run_bal,
		jv_no,  cv_no, pv_no, or_no, ar_no, pfr_no, si_no, remarks, status 
		from 
		(

			/*GET THE BEGINNING BALANCE*/
			select '***' as T_date, 
			'2014-01-01'::date as Gl_date,
			'Beginning Bal.' as description, 
			'' as div_id,  
			'' as dept_id,
			'' as project_id,  
			'' as sub_projectid,
			(case when p_acct_id in (select acct_id from mf_boi_chart_of_accounts) then 
				(case when coalesce(sum(a.balance),00) >= 0 then coalesce(sum(a.balance),00) else 0.00 end) end) as debit,             
			--(case when p_acct_id in (select acct_id from mf_boi_chart_of_accounts where bs_is = 'IS' and acct_id <> '09-01-99-000') then 0 else   --delete condition as per DCRF 812 
			(case when p_acct_id in (select acct_id from mf_boi_chart_of_accounts) then 
				(case when coalesce(sum(a.balance),00) < 0 then coalesce(sum(a.balance),00)*-1 else 0.00 end) end) as credit, 
			0.00 as run_bal,
			'' as jv_no, 
			'' as cv_no,  
			'' as pv_no,  
			'' as or_no,  
			'' as ar_no,  
			'' as pfr_no, 
			'' as si_no,
			'' as remarks,
			'' as project_id2,  
			'' as sub_projectid2,
			'' as status

			from 
			( 

				/*JV - DEBIT*/
				select (case when c.debit is null then 0 else c.debit end ) as balance  
				from  
				(
					select distinct on (b.jv_no)  
					a.jv_date, b.entry_no, a.fiscal_yr,  
					a.period_id, b.tran_amt, b.jv_no, b.line_no, b.bal_side, a.status_id, b.co_id   
					from rf_jv_header a, rf_jv_detail b  
					where a.jv_no = b.jv_no and trim(b.acct_id) = p_acct_id
					and a.jv_date::date < p_date_fr::date 
					and 
					(
						case 
							when a.fiscal_yr < to_char(p_date_fr::date,'YYYY')::int 
						 		then true 
						 	else -- see DCRF No. 260
							(
								case 
									when p_include_month = '' 
										then a.period_id::int <= 12 
									else a.period_id::int <= coalesce(p_include_month,null)::int 
								end
							) 
						 end
					)
					and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
					and a.co_id = p_co_id 
					and b.co_id = p_co_id
					and b.status_id = 'A' 
					and 
					(
						case 
							when to_char(p_date_fr::date,'yyyy') <= '2017' or v_isYearClosed is true
								then to_char(a.jv_date::date,'yyyy') <= to_char(p_date_fr::date,'yyyy')
							else to_char(a.jv_date::date,'yyyy') = to_char(p_date_fr::date,'yyyy')
						end
					) --add'l condition as per DCRF No. 453
					and (case when p_proj_id = '' then b.jv_no is not null else b.project_id = p_proj_id end)
					and (case when p_phase_id = '' then b.jv_no is not null else b.sub_projectid = p_phase_id end)
					and (case when p_period_fr is null then true else a.period_id::int >= p_period_fr end) 
					and (case when p_period_to is null then true else a.period_id::int <= p_period_to end) 	
					order by b.jv_no, b.entry_no  
				) as a 

				join 
				(
					select distinct on (jv_no, co_id) jv_no, bal_side, sum(tran_amt) as debit, co_id   
					from rf_jv_detail where bal_side = 'D' and trim(acct_id) = p_acct_id
					and co_id = p_co_id and status_id = 'A'  
					and (case when p_proj_id = '' then jv_no is not null else project_id = p_proj_id end)
					and (case when p_phase_id = '' then jv_no is not null else sub_projectid = p_phase_id end)
					group by jv_no, co_id, bal_side
				) as c  
				on a.jv_no = c.jv_no and a.co_id = c.co_id 

				UNION ALL

				/*JV - CREDIT*/
				select -1 * (case when b.credit is null then 0 else b.credit end) as balance  
				from  			 
				(
					select distinct on (b.jv_no)  
					a.jv_date, b.entry_no, a.fiscal_yr,  
					a.period_id, b.tran_amt, b.jv_no, b.line_no, b.bal_side, a.status_id, b.co_id   
					from rf_jv_header a, rf_jv_detail b  
					where a.jv_no = b.jv_no and trim(b.acct_id) = p_acct_id
					and a.jv_date::date < p_date_fr::date 
					and 
					(
						case 
						 	when a.fiscal_yr < to_char(p_date_fr::date,'YYYY')::int 
						 		then true 
							else -- see DCRF No. 260
								(
									case 
										when p_include_month = '' 
											then a.period_id::int <= 12 
										else a.period_id::int <= coalesce(p_include_month,null)::int 
								 	end
								) 
						end
					)
					--and a.status_id ='P' 
					and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
					and a.co_id = p_co_id 
					and b.co_id = p_co_id
					and b.status_id = 'A' 
					and 
					(
						case 
							when to_char(p_date_fr::date,'yyyy') <= '2017' or v_isYearClosed is true
								then to_char(a.jv_date::date,'yyyy') <= to_char(p_date_fr::date,'yyyy')
							else to_char(a.jv_date::date,'yyyy') = to_char(p_date_fr::date,'yyyy')
						end
					) --add'l condition as per DCRF No. 453
					and (case when p_proj_id = '' then b.jv_no is not null else b.project_id = p_proj_id end)
					and (case when p_phase_id = '' then b.jv_no is not null else b.sub_projectid = p_phase_id end)
					and (case when p_period_fr is null then true else a.period_id::int >= p_period_fr end) 
					and (case when p_period_to is null then true else a.period_id::int <= p_period_to end) 	
					order by b.jv_no, b.entry_no  
				) as a 

				left join 
				(
					select distinct on (jv_no, co_id) jv_no, bal_side, sum(tran_amt) as credit, co_id  
					from rf_jv_detail where bal_side = 'C' and trim(acct_id) = p_acct_id
					and co_id = p_co_id 
					and status_id = 'A'  
					and (case when p_proj_id = '' then jv_no is not null else project_id = p_proj_id end)
					and (case when p_phase_id = '' then jv_no is not null else sub_projectid = p_phase_id end)
					group by jv_no, co_id, bal_side
				) as b  
				on a.jv_no = b.jv_no and a.co_id = b.co_id  

				UNION ALL 

				select 
				(case when c.debit is null then 0 else c.debit end) as balance  
				from  
				(
					select *
					from rf_cv_header a
					where status_id not in ('I','D')
					and a.date_paid::date < p_date_fr::date  --updated by Del G. 11/03/16
					and a.date_paid is not null --added by Del G. 11/03/16 
					and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
					and a.co_id = p_co_id
					and case when (EXTRACT(year from p_date_fr::TIMESTAMP) <= 2022 and date_paid::date > '2022-01-01'::date ) then true else (a.server_id is null OR remarks ~*'TRANSFERRED FROM ITS REAL') end -- added by erick 2023-09-22
					and cv_no NOT IN ('000060925')
					and 
						(
							case when to_char(p_date_fr::date,'yyyy') <= '2017' or v_isYearClosed is true
							then
							to_char(a.date_paid::date,'yyyy') <= to_char(p_date_fr::date,'yyyy')
							else
							to_char(a.date_paid::date,'yyyy') = to_char(p_date_fr::date,'yyyy')
							end
						) --add'l condition as per DCRF No. 453
				) as a  
				 
				join 
				(
					select distinct on (cv_no, co_id) acct_id, cv_no, bal_side, sum(tran_amt) as debit, co_id   
					from rf_cv_detail  
					where bal_side = 'D' 
					and trim(acct_id) = p_acct_id 
					and co_id = p_co_id 
					and status_id = 'A' 
					group by cv_no, co_id, bal_side, acct_id
				) as c 
				on a.cv_no = c.cv_no and a.co_id = c.co_id 
				where (case when p_proj_id = '' then a.cv_no is not null else c.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details)
				and (case when p_phase_id = '' then a.cv_no is not null else c.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details) 
				--and a.status_id ='P' 

				UNION ALL 

				/*CV - CREDIT*/
				select -1* (case when b.credit is null then 0 else b.credit end) as balance  
				from  
				(
					select *
					from rf_cv_header a
					where status_id not in ('I','D')
					and a.date_paid::date < p_date_fr::date  --updated by Del G. 11/03/16
					and a.date_paid is not null --added by Del G. 11/03/16 
					and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
					and a.co_id = p_co_id
					and case when (EXTRACT(year from p_date_fr::TIMESTAMP) <= 2022 and date_paid::date > '2022-01-01'::date ) then true else (a.server_id is null OR remarks ~*'TRANSFERRED FROM ITS REAL') end -- added by erick 2023-09-22
					and cv_no NOT IN ('000060925')
					and 
					(
						case 
							when to_char(p_date_fr::date,'yyyy') <= '2017' or v_isYearClosed is true
								then to_char(a.date_paid::date,'yyyy') <= to_char(p_date_fr::date,'yyyy')
							else to_char(a.date_paid::date,'yyyy') = to_char(p_date_fr::date,'yyyy')
						end
					) --add'l condition as per DCRF No. 453
				) as a  
				 
				join 
				(
					select distinct on (cv_no, co_id) acct_id, cv_no, bal_side, sum(tran_amt) as credit, co_id   
					from rf_cv_detail  
					where bal_side = 'C' 
					and trim(acct_id) = p_acct_id 
					and co_id = p_co_id 
					and status_id = 'A' 
					group by cv_no, co_id, bal_side,acct_id
				) as b   
				on a.cv_no = b.cv_no and a.co_id = b.co_id
				where (case when p_proj_id = '' then a.cv_no is not null else b.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details)
				and (case when p_phase_id = '' then a.cv_no is not null else b.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details) 
					 
				UNION ALL 

				/*PV - DEBIT*/
				select ( case when c.debit is null then 0 else c.debit end ) as balance  
				from  
				(
					select distinct on (a.pv_no) 
					a.pv_date, 
					b.tran_amt, 
					b.pv_no, 
					b.bal_side, 
					a.remarks, 
					a.status_id, 
					a.co_id, 
					b.project_id, 
					b.sub_projectid  
					from rf_pv_header a, rf_pv_detail b  
					where a.pv_no = b.pv_no 
					and trim(b.acct_id) = p_acct_id 
					and a.pv_date::date < p_date_fr::date  
					and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
					and a.co_id = p_co_id and b.status_id = 'A' 
					and (case when p_proj_id = '' then b.pv_no is not null else b.project_id = p_proj_id end)
					and (case when p_phase_id = '' then b.pv_no is not null else b.sub_projectid = p_phase_id end)
					and 
					(
						case 
							when to_char(p_date_fr::date,'yyyy') <= '2017' or v_isYearClosed is true
								then to_char(a.pv_date::date,'yyyy') <= to_char(p_date_fr::date,'yyyy')
							else to_char(a.pv_date::date,'yyyy') = to_char(p_date_fr::date,'yyyy')
						end
					) --add'l condition as per DCRF No. 453
				) as a  
									 
				join 
				(
					select distinct on (pv_no, co_id) 
					pv_no, 
					bal_side, 
					sum(tran_amt) as debit, 
					co_id    
					from rf_pv_detail  
					where bal_side = 'D' 
					and trim(acct_id) = p_acct_id 
					and co_id = p_co_id 
					and status_id = 'A' 
					and (case when p_proj_id = '' then pv_no is not null else project_id = p_proj_id end)
					and (case when p_phase_id = '' then pv_no is not null else sub_projectid = p_phase_id end)
					group by pv_no, co_id, bal_side
				) as c  
				on a.pv_no = c.pv_no and a.co_id = c.co_id 

				UNION ALL 

				/*PV - CREDIT*/
				select -1*( case when b.credit is null then 0 else b.credit end ) as balance  
				from  
				(
					select distinct on (a.pv_no) a.pv_date, 
					b.tran_amt, 
					b.pv_no, 
					b.bal_side, 
					a.remarks, 
					a.status_id, 
					a.co_id, 
					b.project_id, 
					b.sub_projectid  
					from rf_pv_header a, 
					rf_pv_detail b  
					where a.pv_no = b.pv_no 
					and trim(b.acct_id) = p_acct_id 
					and a.pv_date::date < p_date_fr::date  
					--and a.status_id ='P' 
					and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
					and a.co_id = p_co_id and b.status_id = 'A' 
					and (case when p_proj_id = '' then b.pv_no is not null else b.project_id = p_proj_id end)
					and (case when p_phase_id = '' then b.pv_no is not null else b.sub_projectid = p_phase_id end)
					and 
					(
						case 
							when to_char(p_date_fr::date,'yyyy') <= '2017' or v_isYearClosed is true
								then to_char(a.pv_date::date,'yyyy') <= to_char(p_date_fr::date,'yyyy')
							else to_char(a.pv_date::date,'yyyy') = to_char(p_date_fr::date,'yyyy')
						end
					) --add'l condition as per DCRF No. 453
				) as a  

				left join 
				(
					select distinct on (pv_no, co_id) 
					pv_no, bal_side, sum(tran_amt) as credit, 
					co_id   
					from rf_pv_detail  	
					where bal_side = 'C' 
					and trim(acct_id) = p_acct_id 
					and co_id = p_co_id 
					and status_id = 'A' 
					and (case when p_proj_id = '' then pv_no is not null else project_id = p_proj_id end)
					and (case when p_phase_id = '' then pv_no is not null else sub_projectid = p_phase_id end)	
					group by pv_no, co_id, bal_side
				) as b  
				on a.pv_no = b.pv_no and a.co_id = b.co_id 

				UNION ALL 

				/*CRB*/
				select total as crb_amount 
				from 
				(
					select 
					a.acct_id, 
					a.pay_rec_id, 
					sum(a.crb_amt) as total 
					from 
					(
						select  rb_id, doc_id, 
						acct_id, 
						pay_rec_id,
						 sum(trans_amt) as crb_amt 
						from rf_crb_detail  
						where status_id = 'A' 
						and co_id = p_co_id 
						and trim(acct_id) = p_acct_id
						and nullif(TRIM(rb_id), '') IS NOT NULL
						group by rb_id, doc_id, acct_id
						, pay_rec_id
					) a 
					join 
					(
						select  * 
						from 
							(
								select rb_id,issued_date,co_id,proj_id,phase,doc_id,remarks,status_id, pay_rec_id, reference_no  
								from rf_crb_header 
								where status_id != 'I' and co_id=p_co_id
								and nullif(TRIM(rb_id), '') IS NOT NULL
							)a
						where  
						status_id not in ('I','D') 
						AND NOT EXISTS (select * from issued_garbage_fee where client_seqno = a.reference_no) -- to exclude payments from happywell
						and (case when p_status_id = 'A' then true else status_id = 'P' end)--//Comment by Erick 2019-06-26 	
						and (rb_id, doc_id, pay_rec_id::int) not in  
						(

							select coalesce(si_no, or_no), (case when or_date is NOT null then '01' when si_date is not null then '307' else '03' end), pay_rec_id
							from rf_payments 
							where  
								(
								case when to_char(p_date_fr::date,'yyyy')::int <= 2017
								--case when p_date_fr::date <= '2016-10-26'
									then (remarks like '%JV No.%' and remarks like '%Late%' and to_char((case when coalesce(si_date, or_date) is null then trans_date else coalesce(si_date,or_date) end),'yyyy')::int >= 2017)  
							else ( remarks like '%Late%' and to_char((case when coalesce(si_date, or_date) is null then trans_date else coalesce(si_date, or_date) end),'yyyy')::int >= 2017)
								end 
								)/*ADDED BY ERICK BITUEN DATED 2019-08-08-- TO INCLUDE LATE OR  W/O JV IN THE PERIOD TRANSACTION*/
							and co_id = p_co_id --2022-06-13
							union all

							/*	Added by: Mann2x; Date Added: September 11, 2018; As requested by Hazel that the payments made through client requests should not be included in the GL;	*/
							select coalesce(si_no, or_no), (case when or_date is NOT null then '01' when si_date is not null then '307' else '03' end) , pay_rec_id
							from rf_payments 
							where remarks like '%Special Case%' 
							/*	Modified by Mann2x; Date Modified: January 17; Every payments credited from client requests should not be included;	*/
							--and trans_date::date >= p_date_fr::date
							and trans_date::date >= '2018-01-01'::date
							and co_id = p_co_id --added by lester to filter receipt from different companies 2022-03-23
							--and server_id IS NULL
							union all

							select ar_no, '03', pay_rec_id 
							from rf_payments 
							where --status_id='A' --comment by Erick Bituen dated 10-01-2020
							(status_id = 'A' or (status_id = 'I' and (request_no is not null or coalesce(request_no, '') != '' or request_no != '') and (remarks like '%Special Case Credit of Payment%' or remarks ~* 'Special Case')) ) --added by Erick Bituen 10-01-2020 to include client request with inactive status
							and (request_no is not null and refund_date is null)  and to_char(trans_date,'yyyy')::int >= 2018 and ar_no is not null --added by Erick 2019-07-15 dcrf 1121
							and co_id = p_co_id
							union all
							
							--Added by Erick dated 2021-06-04 another special case reference DCRF 1676, 1678, 1679, 1681
							select or_no, '01', pay_rec_id
							from rf_payments
							where pay_rec_id in ('80684', '80693', '80685', '80695', '80687', '80694', '80686', '81160', '81161')
							and co_id = p_co_id --2022-06-13
						) 
						and case when p_co_id = '01' then a.rb_id not in ('005987','005988','006018B','006019B','006035B','006036B','006037B') else true end -- Added by Erick 2023-11-22 to exclude in GL Hernandez Karen payment
						-- added condition as per DCRF No. 429			
					) b on a.rb_id = b.rb_id and a.doc_id = b.doc_id and a.pay_rec_id::int = b.pay_rec_id::int
					left join 
					(
						select distinct on (or_no, doc_id
						, pay_rec_id
						) or_no, doc_id, or_date
						, pay_rec_id
						from 
						(
							select coalesce(si_no, or_no) as or_no, 
							(case when or_doc_id = '01' then or_date WHEN si_doc_id = '307' THEN si_date else trans_date end) as or_date, 
							(case when or_doc_id = '01' then '01' when si_doc_id = '307' THEN '307' else '03' end) as doc_id, remarks, pay_rec_id
							from rf_payments 
							where status_id = 'A'
							and co_id = p_co_id 
							and server_id IS NULL
						) a 
					) c on a.rb_id = c.or_no and a.doc_id = c.doc_id and a.pay_rec_id::int = c.pay_rec_id::int --added by DG ; 03/13/2017
					where (case when c.or_date is null or to_char(c.or_date,'yy')::int >= 18 then b.issued_date::date else c.or_date::date end) < p_date_fr::date  --updated by Del G. on 11-07-2016 for Late LTS OR
					and (case when p_proj_id = '' then a.rb_id is not null else b.proj_id= p_proj_id end)
					and (case when p_phase_id = '' then a.rb_id is not null else b.phase = p_phase_id end)
					and 
					(
						case 
							when to_char(p_date_fr::date,'yyyy') <= '2017' or v_isYearClosed is true
								then to_char
								(
									(
										case 
											when c.or_date is null or to_char(or_date,'yy')::int >= 18 
												then b.issued_date::date 
											else c.or_date::date 
										end),'yyyy'
									) <= to_char(p_date_fr::date,'yyyy')
							else to_char
								(
									(
										case 
											when c.or_date is null or to_char(or_date,'yy')::int >= 18 
												then b.issued_date::date 
											else c.or_date::date 
										end
									),'yyyy') = to_char(p_date_fr::date,'yyyy')
						end
					) --add'l condition as per DCRF No. 453
					group by a.acct_id, a.pay_rec_id
				) a  
			) a  
			/*END OF BEGINNING BALANCE*/

			UNION ALL

			(
			/*GET THE PERIOD TRANSACTION*/
			select * 
			from 
			(
				select * 
				from 
				( 			

					/*JV-DEBIT*/ 
					select 
					to_char(a.jv_date,'MM-dd-yyyy') as gl_date, 
					a.jv_date::date as jv_date, 
					'' as description,  
					c.div_id, --Added by Erick 10-01-2020
					--a.div_id, --Comment by Erick 10-01-2020
					a.dept_id,
					c.project_id,
					c.sub_projectid,
					case when c.debit is null then 0 else c.debit end,  
					0.00,
					0.00, 
					a.jv_no, 
					'' as cv_no,  
					'' as pv_no,  
					'' as or_no,  
					'' as ar_no, 
					'' as pfr_no, 
					'' as si_no,
					trim(a.remarks) as remarks,
					a.project_id,  
					a.sub_projectid,
					a.status_id

					from  
					( select distinct on (b.jv_no )  --edited by erick bituen dated 09-24-2020 added div_id in distinct to seperate acct. per div_id 
						a.jv_date, b.entry_no, a.fiscal_yr,  
						a.period_id, b.tran_amt, b.jv_no, b.line_no, b.bal_side, a.remarks, a.status_id, b.co_id, b.project_id, b.sub_projectid, b.div_id, b.dept_id
						from rf_jv_header a, rf_jv_detail b  
						where a.jv_no = b.jv_no and trim(b.acct_id) = p_acct_id 
						and a.jv_date::date >= p_date_fr::date 
						and a.jv_date::date <= p_date_to::date  
						and (case when a.fiscal_yr < to_char(p_date_fr::date,'YYYY')::int then true else -- see DCRF No. 260
								(case when p_include_month = '' then a.period_id::int <= 12 else a.period_id::int <= coalesce(p_include_month,null)::int end) end)
						and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
						and a.co_id = p_co_id 
						and b.co_id = p_co_id 
						and b.status_id = 'A' 
						and (case when p_proj_id = '' then b.jv_no is not null else b.project_id = p_proj_id end)
						and (case when p_phase_id = '' then b.jv_no is not null else b.sub_projectid = p_phase_id end)
						and (case when p_period_fr is null then true else a.period_id::int >= p_period_fr end) 
						and (case when p_period_to is null then true else a.period_id::int <= p_period_to end) 
						and  b.tran_amt >0  --Added by Erick dated 07-04-2019 zero trans_amt should not be displayed.--requested by Mam Weng	
						order by b.jv_no, b.entry_no  
						) as a 
						 
					join ( --select distinct on (jv_no, co_id, project_id, sub_projectid, div_id, dept_id) jv_no, bal_side, sum(tran_amt) as debit, co_id, project_id, sub_projectid, div_id   --edited by erick bituen dated 09-24-2020 added div_id in distinct to seperate acct. per div_id 
						select jv_no, bal_side, tran_amt as debit, co_id, project_id, sub_projectid, div_id -- Edited by Erick 2023/03/02 DCRF 2493
						from rf_jv_detail where bal_side = 'D' and trim(acct_id) = p_acct_id 
						and co_id = p_co_id and status_id = 'A' 
						and (case when p_proj_id = '' then jv_no is not null else project_id = p_proj_id end)
						and (case when p_phase_id = '' then jv_no is not null else sub_projectid = p_phase_id end) ) as c
						--group by jv_no, bal_side, co_id, project_id, sub_projectid, div_id, dept_id, tran_amt ) as c  --Comment by Erick 2023/03/08
						on a.jv_no = c.jv_no and a.co_id = c.co_id  --Added by erick bituen dated 09-24-2020 added div_id as filter 

					UNION ALL 

					/*JV-CREDIT*/ 
					select 
					to_char(a.jv_date,'MM-dd-yyyy') as gl_date,  
					a.jv_date::date as jv_date,
					'' as description,  
					b.div_id, --Added by Erick 10-01-2020
					--a.div_id, --Comment by Erick 10-01-2020
					a.dept_id,
					b.project_id,
					b.sub_projectid,
					0.00,
					case when b.credit is null then 0 else b.credit end,   
					0.00, 
					a.jv_no, 
					'' as cv_no,  
					'' as pv_no,  
					'' as or_no,  
					'' as ar_no, 
					'' as pfr_no, 
					'' as si_no,
					trim(a.remarks) as remarks,
					a.project_id,  
					a.sub_projectid,
					a.status_id

					from  
					( select distinct on (b.jv_no)  --edited by erick bituen dated 09-24-2020 added div_id in distinct to seperate acct. per div_id 
						a.jv_date, b.entry_no, a.fiscal_yr,  
						a.period_id, b.tran_amt, b.jv_no, b.line_no, b.bal_side, a.remarks, a.status_id, b.co_id, b.project_id, b.sub_projectid, b.div_id, b.dept_id
						from rf_jv_header a, rf_jv_detail b  
						where a.jv_no = b.jv_no and trim(b.acct_id) = p_acct_id 
						and a.jv_date::date >= p_date_fr::date 
						and a.jv_date::date <= p_date_to::date  
						and (case when a.fiscal_yr < to_char(p_date_fr::date,'YYYY')::int then true else -- see DCRF No. 260
								(case when p_include_month = '' then a.period_id::int <= 12 else a.period_id::int <= coalesce(p_include_month,null)::int end) end)
						and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
						and a.co_id = p_co_id 
						and b.co_id = p_co_id 
						and b.status_id = 'A' 
						and (case when p_proj_id = '' then b.jv_no is not null else b.project_id = p_proj_id end)
						and (case when p_phase_id = '' then b.jv_no is not null else b.sub_projectid = p_phase_id end)
						and (case when p_period_fr is null then true else a.period_id::int >= p_period_fr end) 
						and (case when p_period_to is null then true else a.period_id::int <= p_period_to end) 
						and  b.tran_amt >0  --Added by Erick dated 07-04-2019 zero trans_amt should not be displayed.--requested by Mam Weng	
						order by b.jv_no, b.entry_no  
						) as a 
					
					join (--select distinct on (jv_no, co_id, project_id, sub_projectid, div_id, dept_id ) jv_no, bal_side, sum(tran_amt) as credit, co_id, project_id, sub_projectid, div_id, dept_id   --edited by erick bituen dated 09-24-2020 added div_id in distinct to seperate acct. per div_id   
						select jv_no, bal_side, tran_amt as credit, co_id, project_id, sub_projectid, div_id -- Edited by Erick 2023/03/02 DCRF 2493
						from rf_jv_detail where bal_side = 'C' and trim(acct_id) = p_acct_id 
						and co_id = p_co_id and status_id = 'A'  
						and (case when p_proj_id = '' then jv_no is not null else project_id = p_proj_id end)
						and (case when p_phase_id = '' then jv_no is not null else sub_projectid = p_phase_id end) ) as b
						--group by jv_no, bal_side, co_id, project_id, sub_projectid, div_id, dept_id, tran_amt ) as b --Comment by Erick 2023/03/08 
						on a.jv_no = b.jv_no and a.co_id = b.co_id  --Added by erick bituen dated 09-24-2020 added div_id as filter 

					UNION ALL 

					/*CV-DEBIT*/
					select  
					to_char(a.date_paid,'MM-dd-yyyy') as date, 
					a.date_paid::date as date_paid,
					'' as description, 
					'' as div_id,
					'' as dept_id,
					'' as project_id,  
					'' as sub_projectid,	
					case when c.debit is null then 0 else c.debit end,   	
					0.00,  --credit
					0.00, 
					'' as jv_no,  
					a.cv_no, 
					'' as pv_no, 
					'' as or_no,  
					'' as ar_no,  
					'' as pfr_no, 
					'' as si_no,
					trim(a.remarks) as remarks, 
					'', 
					'',
					a.status_id  
				 
					from 
					(select distinct on (a.cv_no) a.date_paid, b.tran_amt, b.cv_no, b.bal_side, a.remarks, a.status_id, a.co_id 
						from rf_cv_header a, rf_cv_detail b  
						where a.cv_no = b.cv_no and trim(b.acct_id) = p_acct_id  
						and a.date_paid is not null --added by Del G. 11/03/16 
						and a.date_paid::date >= p_date_fr::date   --updated by Del G. 11/03/16 
						and a.date_paid::date <= p_date_to::date   --updated by Del G. 11/03/16 		
						and (case when p_proj_id = '' then a.cv_no is not null else b.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details)
						and (case when p_phase_id = '' then a.cv_no is not null else b.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details) 
						--and a.status_id ='P'
					 	--and (a.remarks ~*'TRANSFERRED FROM ITS REAL' or (CASE WHEN EXTRACT(year from p_date_fr::TIMESTAMP) >= 2022 THEN TRUE ELSE a.server_id is null END))  -- Comment by Erick 2023/02/07
						and case when (EXTRACT(year from p_date_fr::TIMESTAMP) >= 2022 and date_paid::date < '2022-01-01'::date ) then true else (a.server_id is null OR remarks ~*'TRANSFERRED FROM ITS REAL') end --Added by Erick 2023/02/07
					 	and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
						and a.co_id = p_co_id 
						and b.co_id = p_co_id
						and b.status_id = 'A' 
					) as a  
							 
					join 
					(select cv_no, bal_side, tran_amt as debit, co_id  
						from rf_cv_detail  
						where bal_side = 'D' and trim(acct_id) = p_acct_id and co_id = p_co_id and status_id = 'A' 
					) as c on a.cv_no = c.cv_no and a.co_id = c.co_id  

					UNION ALL

					/*CV-CREDIT*/
					select  
					to_char(a.date_paid,'MM-dd-yyyy') as date, 
					a.date_paid::date as date_paid,
					'' as description, 
					'' as div_id,
					'' as dept_id,
					'' as project_id,  
					'' as sub_projectid,	
					0.00, --debit
					case when b.credit is null then 0 else b.credit end, 
					0.00, 
					'' as jv_no,  
					a.cv_no, 
					'' as pv_no, 
					'' as or_no,  
					'' as ar_no,  
					'' as pfr_no, 
					'' as si_no,
					trim(a.remarks) as remarks, 
					'', 
					'',
					a.status_id  
				 
					from 
					(select distinct on (a.cv_no) a.date_paid, b.tran_amt, b.cv_no, b.bal_side, a.remarks, a.status_id, a.co_id 
						from rf_cv_header a, rf_cv_detail b  
						where a.cv_no = b.cv_no and trim(b.acct_id) = p_acct_id  
						and a.date_paid is not null --added by Del G. 11/03/16 
						and a.date_paid::date >= p_date_fr::date   --updated by Del G. 11/03/16 
						and a.date_paid::date <= p_date_to::date   --updated by Del G. 11/03/16 		
						and (case when p_proj_id = '' then a.cv_no is not null else b.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details)
						and (case when p_phase_id = '' then a.cv_no is not null else b.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details) 
						--and a.status_id ='P'
					 	--and (a.remarks ~*'TRANSFERRED FROM ITS REAL'or (CASE WHEN EXTRACT(year from p_date_fr::TIMESTAMP) >= 2022 THEN TRUE ELSE a.server_id is null END)) -- Comment by Erick 2023/02/07
						and case when (EXTRACT(year from p_date_fr::TIMESTAMP) >= 2022 and date_paid::date < '2022-01-01'::date ) then true else (a.server_id is null OR remarks ~*'TRANSFERRED FROM ITS REAL') end --Added by Erick 2023/02/07
					 	and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
						and a.co_id = p_co_id 
						and b.co_id = p_co_id
						and b.status_id = 'A' 
					) as a  
								 
					join 
					(select cv_no, bal_side, tran_amt as credit, co_id  
						from rf_cv_detail  
						where bal_side = 'C' and trim(acct_id) = p_acct_id and co_id = p_co_id and status_id = 'A' 
					) as b  on a.cv_no = b.cv_no and a.co_id = b.co_id  
						

					UNION ALL 

					/*PV-DEBIT*/
					select  
					to_char(a.pv_date,'MM-dd-yyyy') as date, 
					a.pv_date::date as pv_date,
					'' as description, 
					--a.div_id,
					--a.dept_id,
					c.div_id,  --added by erick 2021-06-09
					c.dept_id, --added by erick 2021-06-09
					(case when c.project_id is null then a.project_id else c.project_id end) as project_id,  
					(case when c.sub_projectid is null then a.sub_projectid else c.sub_projectid end) as sub_projectid, 
					case when c.debit is null then 0 else c.debit end,   
					--case when b.credit is null then 0 else b.credit end, 
					0.00, --credit
					0.00, 
					'' as jv_no,  
					'' as cv_no,  
					a.pv_no, 
					'' as or_no,  
					'' as ar_no,  
					'' as pfr_no, 
					'' as si_no,
					trim(a.remarks) as remarks,
					a.project_id,  
					a.sub_projectid,
					a.status_id   
				 
					from  
					(select distinct on (a.pv_no) a.pv_date, b.tran_amt, b.pv_no, b.bal_side, a.remarks, a.status_id, a.co_id, b.project_id, b.sub_projectid, div_id, dept_id 
						from rf_pv_header a, rf_pv_detail b  
						where a.pv_no = b.pv_no and trim(b.acct_id) = p_acct_id  
						and a.pv_date::date >= p_date_fr::date  
						and a.pv_date::date <= p_date_to::date		 
						--and a.status_id ='P' 
						and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
						and a.co_id = p_co_id 
						and b.co_id = p_co_id
						and b.status_id = 'A'
						and TRIM(a.pv_no) != ''
						AND TRIM(b.pv_no) != ''
						--and a.remarks !~*'CMD INITIATED BILLING'
						and (case when p_proj_id = '' then b.pv_no is not null else b.project_id = p_proj_id end)
						and (case when p_phase_id = '' then b.pv_no is not null else b.sub_projectid = p_phase_id end)
					) as a  

					/*		 
					left join (select pv_no, bal_side, tran_amt as credit, co_id, project_id, sub_projectid  
						from rf_pv_detail  
						where bal_side = 'C' and trim(acct_id) = p_acct_id and co_id = p_co_id and status_id = 'A' 
						and (case when p_proj_id = '' then pv_no is not null else project_id = p_proj_id end)
						and (case when p_phase_id = '' then pv_no is not null else sub_projectid = p_phase_id end)
						) as b  
						on a.pv_no = b.pv_no and a.co_id = b.co_id  
					*/
									 
					join 
					(select pv_no, bal_side, tran_amt as debit, co_id, project_id, sub_projectid,
					 	dept_id, --added by erick 2021-06-09
					 	div_id   --added by erick 2021-06-09
						from rf_pv_detail  
						where bal_side = 'D' and trim(acct_id) = p_acct_id and co_id = p_co_id and status_id = 'A' 
						and (case when p_proj_id = '' then pv_no is not null else project_id = p_proj_id end)
						and (case when p_phase_id = '' then pv_no is not null else sub_projectid = p_phase_id end)
					) as c  
						on a.pv_no = c.pv_no and a.co_id = c.co_id 

					UNION ALL 

					/*PV-CREDIT*/
					select  
					to_char(a.pv_date,'MM-dd-yyyy') as date, 
					a.pv_date::date as pv_date,
					'' as description, 
					--a.div_id,
					--a.dept_id,
					b.div_id,  --added by erick 2021-06-09
					b.dept_id, --added by erick 2021-06-09
					(case when b.project_id is null then a.project_id else b.project_id end) as project_id,  
					(case when b.sub_projectid is null then a.sub_projectid else b.sub_projectid end) as sub_projectid, 
					--case when c.debit is null then 0 else c.debit end,   
					0.00, --debit
					case when b.credit is null then 0 else b.credit end, 	
					0.00, 
					'' as jv_no,  
					'' as cv_no,  
					a.pv_no, 
					'' as or_no,  
					'' as ar_no,  
					'' as pfr_no, 
					'' as si_no,
					trim(a.remarks) as remarks,
					a.project_id,  
					a.sub_projectid,
					a.status_id   
				 
					from  
					(select distinct on (a.pv_no) a.pv_date, b.tran_amt, b.pv_no, b.bal_side, a.remarks, a.status_id, a.co_id, b.project_id, b.sub_projectid, div_id, dept_id 
						from rf_pv_header a, rf_pv_detail b  
						where a.pv_no = b.pv_no and trim(b.acct_id) = p_acct_id  
						and a.pv_date::date >= p_date_fr::date  
						and a.pv_date::date <= p_date_to::date		 
						--and a.status_id ='P' 
						and (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
						and a.co_id = p_co_id 
						and b.co_id = p_co_id
						and b.status_id = 'A'
						and TRIM(a.pv_no) != ''
						AND TRIM(b.pv_no) != ''
					    --and a.remarks !~*'CMD INITIATED BILLING'
						and (case when p_proj_id = '' then b.pv_no is not null else b.project_id = p_proj_id end)
						and (case when p_phase_id = '' then b.pv_no is not null else b.sub_projectid = p_phase_id end)
						) as a  
									 
					join 
					(
					select pv_no, 
					bal_side, 
					tran_amt as credit, 
					co_id, project_id, 
					sub_projectid,
					dept_id,  --added by erick 2021-06-09
					div_id    --added by erick 2021-06-09
					from rf_pv_detail  
					where bal_side = 'C' 
					and trim(acct_id) = p_acct_id 
					and co_id = p_co_id 
					and status_id = 'A'
					and (case when p_proj_id = '' then pv_no is not null else project_id = p_proj_id end)
					and (case when p_phase_id = '' then pv_no is not null else sub_projectid = p_phase_id end)
					) as b  
					on a.pv_no = b.pv_no and a.co_id = b.co_id  

					/*				 
					left join (select pv_no, bal_side, tran_amt as debit, co_id, project_id, sub_projectid
						from rf_pv_detail  
						where bal_side = 'D' and trim(acct_id) = p_acct_id and co_id = p_co_id and status_id = 'A' 
						and (case when p_proj_id = '' then pv_no is not null else project_id = p_proj_id end)
						and (case when p_phase_id = '' then pv_no is not null else sub_projectid = p_phase_id end)
						) as c  
						on a.pv_no = c.pv_no and a.co_id = c.co_id 
					*/

					UNION ALL 

					/*CRB*/
					select --distinct on (a.rb_id)
					to_char(a.date,'MM-dd-yyyy'),  
					a.date::date as date,
					d.description, 
					'' as div,
					'' as dept,
					a.proj_id,  
					a.phase,
					(case when a.trans_amt <=0 then 0 else a.trans_amt end ) as debit,    
					( case when a.trans_amt >=0 then 0 else a.trans_amt*-1 end ) as credit,  
					0, 
					'' as jv_no,  
					'' as cv_no, 
					'' as pv_no, 
					( case when a.doc_id = '01' then a.rb_id else '' end ) as or_no,  
					( case when a.doc_id = '03' then a.rb_id else '' end ) as ar_no,  
					'' as pfr_no,
					( case when a.doc_id not in ('01','03') then a.rb_id else '' end ) as si_no,  
					trim(a.remarks), 
					a.proj_id,  
					a.phase,
					a.status_id
				 
					from   
					(
						select 
						--a.issued_date 
						(case when c.or_date is null or to_char(c.or_date,'yy')::int >= 18 then a.issued_date::date else c.or_date::date end) as date, -- adjusted by DG on 04/11/2017
						b.rb_fiscal_year, 
						b.rb_month, 
						b.trans_amt, 
						b.rb_id, b.line_no, 
						--coalesce(d.remarks,a.remarks) as remarks,
						/*
						coalesce((SELECT remarks from rf_crb_special_remarks where branch_id = c.branch_id and trans_date::date = a.issued_date::date and co_id = b.co_id),a.remarks)
						modified by jari cruz asof nov 18 2022
						*/
						--coalesce((SELECT remarks from rf_crb_special_remarks where branch_id = c.branch_id and trans_date::date = a.issued_date::date and co_id = b.co_id),a.remarks || (case when nullif(TRIM(c.branch_id),'') is not null then format(', %s %s',(select branch_alias from mf_office_branch where branch_id = c.branch_id),'BRANCH') end)) as remarks,
						--a.remarks, 
						case 
							when 
								exists(SELECT remarks from rf_crb_special_remarks where branch_id = c.branch_id and trans_date::date = a.issued_date::date and co_id = b.co_id)
							then 
								(SELECT remarks from rf_crb_special_remarks where branch_id = c.branch_id and trans_date::date = a.issued_date::date and co_id = b.co_id)
							else 
								(case 
									when nullif(c.branch_id,'') is null 
									then 
										a.remarks || ' ' || (case when a.particulars = 'CR-LIQ' then (select entity_name from rf_entity where entity_id = b.entity_id) end)
									else
										(a.remarks || ', ' ||(select branch_alias || ' BRANCH' from mf_office_branch where branch_id = c.branch_id))
								end)
									
							end as remarks,	
						a.status_id, 
						a.doc_id, 
						b.pbl_id, 
						a.co_id, 
						a.proj_id, 
						a.phase 
						from 
						(
							select --distinct on (rb_id, doc_id) 
							a.rb_id, 
							a.issued_date, 
							a.co_id, 
							a.proj_id, 
							a.phase, 
							a.doc_id, 
							/*
							a.remarks,
							modified by jari cruz asof nov 18 2022
							*/
							(select 'To record the collection for ' || a.issued_date::date) as remarks, 
							a.status_id, a.pay_rec_id, a.particulars 
							from (select * from rf_crb_header where status_id != 'I' and co_id = p_co_id AND pay_rec_id NOT IN ('520700', '520759', '521236', '521237', '520701', '520760', '521238')) a
							--where (case when p_status_id = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 	
							where trim(status_id) not in ('I','D') 
							AND NOT EXISTS (select * from issued_garbage_fee where client_seqno = a.reference_no)
							and (case when p_status_id = 'A' then true else status_id = 'P' end ) 			
							--and (case when p_status_id = 'A' then true else status_id = 'P' end ) --Comment by Erick 2019-06-18			
							and a.co_id = p_co_id 
							and (case when p_proj_id = '' then a.rb_id is not null else a.proj_id = p_proj_id end)
							and (case when p_phase_id = '' then a.rb_id is not null else a.phase  = p_phase_id end)
							and not exists (SELECT * 
										    FROM issued_garbage_fee
										    where client_seqno = a.reference_no
										    and status_id = 'A')
							and (rb_id, doc_id, pay_rec_id::INT) not in ( 
							
									select coalesce(si_no,or_no), 
									(case when or_date is not null then '01' when si_date is not null then '307'  else '03' end), pay_rec_id 
									from rf_payments 
									--where (remarks like '%JV No.%' and remarks like '%Late%' and to_char((case when or_date is null then trans_date else or_date end),'yyyy')::int >= 2017)
									where --( remarks like '%Late%' and to_char((case when or_date is null then trans_date else or_date end),'yyyy')::int >= 2017)
									--or (request_no is not null and to_char((case when or_date is null then trans_date else or_date end),'yyyy')::int >= 2017)
										(
										case when to_char(p_date_fr::date,'yyyy')::int = 2017 
										--case when p_date_fr::date <= '2016-10-26'
												then (remarks like '%JV No.%' and remarks like '%Late%' and to_char((case when or_date is null then trans_date else or_date end),'yyyy')::int >= 2017)  
											else ( remarks like '%Late%' and to_char((case when or_date is null then trans_date else or_date end),'yyyy')::int >= 2017)
										end 
										)/*ADDED BY ERICK BITUEN DATED 2019-08-08-- TO INCLUDE LATE OR  W/O JV IN THE BEGINNING BALANCE*/
									and co_id = p_co_id --added by lester to filter receipt from different companies 2022-03-23
									--and server_id is null Comment by Erick 2023-03-29
								
									union all

									/*	Added by: Mann2x; Date Added: September 11, 2018; As requested by Hazel that the payments made through client requests should not be included in the GL;	*/
									select coalesce(si_no,or_no), 
									(case when or_date is not null then '01' when si_date is not null then '307'  else '03' end), pay_rec_id 
									from rf_payments 
									where remarks like '%Special Case%'  
									/*	Modified by Mann2x; Date Modified: January 17; Every payments credited from client requests should not be included;	*/
									and trans_date::date >= '2018-01-01'::date
									and co_id = p_co_id --added by lester to filter receipt from different companies 2022-03-23
									--and server_id is null
								
									union all

									select ar_no, '03', pay_rec_id
									from rf_payments 
									where --status_id='A' --Comment by Erick Bituen 10-01-2020
									(status_id = 'A' or (status_id = 'I' and (request_no is not null or coalesce(request_no, '') != '' or request_no != '') and (remarks like '%Special Case Credit of Payment%' or remarks ~* 'Special Case')) ) --added by Erick Bituen 10-01-2020 to include client request with inactive status
									and (request_no is not null and refund_date is null)   and to_char(trans_date,'yyyy')::int >= 2018 and ar_no is not null --Added by Erick 2019-07-15 dcrf 1121 --Edited by Erick Bituen dated 09-28-2020 To exclude client request for refund of payment
									and co_id = p_co_id --added by lester to filter receipt from different companies 2022-03-23
									--and server_id is null
									--and (case when request_no is not null then refund_date is  null end )
									--and (case when request_no is not null then remarks like'%Refund of Payment' end ) --added by Erick Bituen dated 09-28-2020 To exclude client request for refund of payment
									--request_no is not null and to_char(trans_date,'yyyy')::int >= 2018 and ar_no is not null
								
									union all
									
									--Added by Erick dated 2021-06-04  another special case reference DCRF 1676, 1678, 1679, 1681
									select or_no, '01', pay_rec_id
									from rf_payments
									--where remarks ~* 'Adjustment JV'
									where pay_rec_id in ('80684', '80693', '80685', '80695', '80687', '80694', '80686', '81160', '81161')
									and co_id = p_co_id --added by lester to filter receipt from different companies 2022-03-23	
									--and server_id is null
									) -- added condition as per DCRF No. 429	
							and case when p_co_id = '01' then a.rb_id not in ('005987','005988','006018B','006019B','006035B','006036B','006037B') else true end -- Added by Erick 2023-11-22 to exclude in GL Hernandez Karen payment
						)a
						left join 
						(
							select * 
							from rf_crb_detail 
							where (rb_id, pay_rec_id, status_id) not in (select rb_id, pay_rec_id, status_id from rf_crb_header where status_id = 'I' AND co_id = p_co_id)
							and co_id = p_co_id 
							and  trans_amt not in ('0') --Added by Erick dated 07-04-2019 zero trans_amt should not be displayed.--requested by Mam Weng
						) b  on a.rb_id = b.rb_id and a.doc_id = b.doc_id and a.pay_rec_id::INT = b.pay_rec_id::INT --ADDED PAY REC_ID BY LESTER BECAUSE INACTIVE ENTRIES ARE DISPLAYED
						left join 
						(
							select distinct on (or_no, doc_id) or_no, doc_id, or_date, branch_id, trans_date from 
							(
								--Edited by Erick 2023/02/02 si_no,si_date,  '307' on doc_id column 
								select coalesce(or_no, si_no) as or_no, 
								(
									case 
										when or_doc_id = '01' 
											then or_date 
										when si_date is not null 
											then si_date  
										else trans_date 
									end
								) as or_date, 
								/*(
									case 
										when or_doc_id is not null 
											then
												(
												case 
													when or_doc_id = '01' 
														then '01'  
													when or_doc_id = '03' 
														then '03' 
													else '307' 
												 end
												)
											else
												(case 
													when pr_doc_id = '02'
														then '02'
													when pr_doc_id = '03'
														then '03'
												end)
									end
								)
								as doc_id, */
								(case when or_doc_id = '01' then '01'   when or_doc_id = '03' then '03' else '307' end) as doc_id, 
								remarks,branch_id, trans_date 
								from rf_payments 
								where status_id = 'A'
								and co_id = p_co_id 
								--where (status_id = 'A' or (status_id = 'I' and (request_no is not null or coalesce(request_no, '') != '' or request_no != '')))
							) a
						) c on b.rb_id = c.or_no and a.doc_id = c.doc_id --ADD PAY REC_ID HERE IF POSSIBLE		--added by Del G. on 11-07-2016 for Late LTS OR
						--left join rf_crb_special_remarks d on c.branch_id = d.branch_id and d.trans_date::date = a.issued_date::date
						where trim(b.acct_id) = p_acct_id  		
						and b.status_id = 'A' 
						and (case when c.or_date is null or to_char(c.or_date,'yy')::int >= 18 then a.issued_date::date else c.or_date::date end)>= p_date_fr::date	--updated by Del G. on 11-07-2016 for Late LTS OR
						and (case when c.or_date is null or to_char(c.or_date,'yy')::int >= 18 then a.issued_date::date else c.or_date::date end) <= p_date_to::date  --updated by Del G. on 11-07-2016 for Late LTS OR	
					) as a    				 
					left join mf_unit_info d on a.proj_id = d.proj_id and a.pbl_id = d.pbl_id 
					
				) a order by a.gl_date::date
		) a 
		order by a.gl_date::date
		)	
			/*END OF PERIOD TRANSACTION*/
		) Z 

		/**I removed this part upon observing that multiple rows (with similar detail) are returned as a single row : DCRF No. 713**/
		--group by gl_date, description, div_id, dept_id, project_id, sub_projectid, debit, credit, 
		--jv_no,  cv_no, pv_no, or_no, ar_no, pfr_no, remarks,  status

		order by GL_date::date

	-------------------------------END GENERAL LEDGER SQL
	) LOOP

		c_T_date	:= v_recGL.T_date;
		c_description	:= v_recGL.description; 
		c_debit		:= v_recGL.debit;
		c_credit	:= v_recGL.credit; 
		c_run_bal	:= v_recGL.run_bal; 
		c_jv_no		:= v_recGL.jv_no;
		c_cv_no		:= v_recGL.cv_no;
		c_pv_no		:= v_recGL.pv_no;
		c_or_no		:= v_recGL.or_no;
		c_ar_no		:= v_recGL.ar_no;
		c_pfr_no	:= v_recGL.pfr_no;
		c_si_no		:= v_recGL.si_no;
		c_remarks	:= v_recGL.remarks;
		c_project_id	:= v_recGL.project_id;
		c_sub_projectid	:= v_recGL.sub_projectid;
		c_status 	:= v_recGL.status;
		c_div	 	:= v_recGL.div_id;
		c_dept 		:= v_recGL.dept_id;

		/*	Added by Mann2x; Date Added: August 16, 2018; DCRF# 705;	*/
		/*	Modified by Erick; Date Added: September 14, 2018; Added company as filter;	*/
		--c_payee := (select y.entity_name from rf_pv_header x inner join rf_entity y on x.entity_id1 = y.entity_id where x.pv_no = c_pv_no limit 1); 
		--c_payee := (select y.entity_name from rf_pv_header x inner join rf_entity y on x.entity_id1 = y.entity_id where (case when c_pv_no is null then x.cv_no = c_cv_no else x.pv_no = c_pv_no end)  and x.co_id=p_co_id limit 1); 
		/*
		raise info ''; 
		raise info 'c_pv_no: %', c_pv_no; 
		raise info 'coalesce(c_pv_no) != '': %', coalesce(c_pv_no) != ''; 
		*/
		--Added by Erick dated 2019-06-20 to display payee names	
		/*if coalesce(c_pv_no, '') != '' 
		then
		c_payee := (select y.entity_name from rf_pv_header x inner join rf_entity y on x.entity_id1 = y.entity_id where  x.pv_no = c_pv_no   and x.co_id=p_co_id limit 1); 
		else
		c_payee := (select y.entity_name from rf_cv_header x inner join rf_entity y on x.entity_id1 = y.entity_id where  x.cv_no = c_cv_no   and x.co_id=p_co_id limit 1); 
		end if;*/
		
		/*ADDED BY ERICK 2021-08-20 DCRF # 1735 */
		RAISE INFO 'c_pv_no: %',c_pv_no;
		RAISE INFO 'c_cv_no: %',c_cv_no;
		RAISE INFO 'c_jv_no: %',c_jv_no;
		RAISE INFO 'c_debit: %',c_debit;
		RAISE INFO 'c_div: %',c_div;
		c_payee := (case when coalesce(c_pv_no, '') != '' 
				   			then (
									--Comment by Erick 2021-11-22
									/*select y.entity_name 
									from rf_pv_header x 
									inner join rf_entity y on x.entity_id1 = y.entity_id 
									where  x.pv_no = c_pv_no   and x.co_id=p_co_id limit 1*/
								
									--Added by Erick 2021-11-22 DCRF No. 1863
									select 
									d.entity_name
									from rf_request_detail a
									left join mf_boi_chart_of_accounts b on a.acct_id=b.acct_id
									left join mf_project c on a.project_id = c.proj_id
									left join rf_entity d on a.entity_id = d.entity_id
									left join mf_entity_type e on a.entity_type_id = e.entity_type_id
									where a.rplf_no = c_pv_no and a.co_id = p_co_id and a.status_id = 'A' and a.div_id = c_div 
									and case when TRIM(p_acct_id) = '01-99-03-000' then a.vat_amt != 0.00 else true end 
									order by a.line_no  limit 1
								 )
						 when coalesce(c_jv_no,'') != '' 
							then
							case when p_acct_id = '01-99-03-000' then --ADJUSTED BY LESTER 2024-01-19 FOR PROPER DISPLAY OF PAYEE WHEN ACCOUNT IS INPUT VAT
								(select b.entity_name from rf_subsidiary_ledger  a
												left join rf_entity b on a.entity_id = b.entity_id
												where a.status_id = 'A'
												--and a.tran_type = '00011'-- Liquidation of Cash Advance Only
												and a.tran_type in ( -- All transaction type with payee-- Requested by Orly 10-21-2021 Additional request for DCRF 1735
																		select distinct on (tran_type)tran_type
																		from rf_subsidiary_ledger
																		where entity_id is not null and status_id = 'A' 
																	)
												and a.co_id = p_co_id
												and a.jv_no = c_jv_no
												and a.vat_amt = c_debit
								 				and a.div_id = c_div
								 				limit 1
								)
							else
								(select b.entity_name from rf_subsidiary_ledger  a
												left join rf_entity b on a.entity_id = b.entity_id
												where a.status_id = 'A'
												--and a.tran_type = '00011'-- Liquidation of Cash Advance Only
												and a.tran_type in ( -- All transaction type with payee-- Requested by Orly 10-21-2021 Additional request for DCRF 1735
																		select distinct on (tran_type)tran_type
																		from rf_subsidiary_ledger
																		where entity_id is not null and status_id = 'A' 
																	)
												and a.co_id = p_co_id
												and a.jv_no = c_jv_no
 								 				and case 
								 						--Added by Erick 2024-09-13 to get the payee of first sl entry of the filtered acct. ID if exists in the specified bal_side(Credit/Debit) as per instruction of Orly
								 						  when  exists( select acct_id from rf_jv_detail 
																	   where jv_no = c_jv_no and co_id = p_co_id and bal_side = 'C' and status_id = 'A'
																	   and acct_id in ('03-01-01-004', 	--Accrued Expense Payable
																					   '03-01-06-002', 	--Withholding Tax Payable - Expanded
																					   '03-01-01-001'	--Accounts Payable - Trade
																					  ))
								 							then true
								 						  when exists( select acct_id from rf_jv_detail 
																	  where jv_no = c_jv_no and co_id = p_co_id and bal_side = 'D' and status_id = 'A'
																	  and acct_id in ('01-99-03-000',	--Input VAT 
																					  '01-99-06-000', 	--Input Vat - Clearing
																					  '01-99-07-000'	--Input Vat - Accrual
																					 ))
 								 							then true
								 					else 
								 						sundry_acct = p_acct_id
								 					end
								 				--and sundry_acct = p_acct_id --  added by erick 2024-08-19 to get the correct payee
												limit 1)
							end 
						else(select y.entity_name 
							 from rf_cv_header x inner 
							 join rf_entity y on x.entity_id1 = y.entity_id 
							 where  x.cv_no = c_cv_no   and x.co_id=p_co_id limit 1)
				   end);
				   
		 
		RETURN NEXT;
	END LOOP;
END;
$BODY$;

ALTER FUNCTION public.view_gen_ledger_detailed_includeactive_v4_debug_erick(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer, integer)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_gen_ledger_detailed_includeactive_v4_debug_erick(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer, integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_gen_ledger_detailed_includeactive_v4_debug_erick(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer, integer) TO employee;

GRANT EXECUTE ON FUNCTION public.view_gen_ledger_detailed_includeactive_v4_debug_erick(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer, integer) TO postgres;

