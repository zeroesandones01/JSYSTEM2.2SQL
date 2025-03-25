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
			(case when '11-01-00-000' in (select acct_id from mf_boi_chart_of_accounts) then 
				(case when coalesce(sum(a.balance),00) >= 0 then coalesce(sum(a.balance),00) else 0.00 end) end) as debit,             
			--(case when '11-01-00-000' in (select acct_id from mf_boi_chart_of_accounts where bs_is = 'IS' and acct_id <> '09-01-99-000') then 0 else   --delete condition as per DCRF 812 
			(case when '11-01-00-000' in (select acct_id from mf_boi_chart_of_accounts) then 
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
				-- select (case when c.debit is null then 0 else c.debit end ) as balance  
				-- from  
				-- (
				-- 	select distinct on (b.jv_no)  
				-- 	a.jv_date, b.entry_no, a.fiscal_yr,  
				-- 	a.period_id, b.tran_amt, b.jv_no, b.line_no, b.bal_side, a.status_id, b.co_id   
				-- 	from rf_jv_header a, rf_jv_detail b  
				-- 	where a.jv_no = b.jv_no and trim(b.acct_id) = '11-01-00-000'
				-- 	and a.jv_date::date < '2024-01-01'::date 
				-- 	and 
				-- 	(
				-- 		case 
				-- 			when a.fiscal_yr < to_char('2024-01-01'::date,'YYYY')::int 
				-- 		 		then true 
				-- 		 	else -- see DCRF No. 260
				-- 			(
				-- 				case 
				-- 					when '14' = '' 
				-- 						then a.period_id::int <= 12 
				-- 					else a.period_id::int <= coalesce('14',null)::int 
				-- 				end
				-- 			) 
				-- 		 end
				-- 	)
				-- 	and (case when 'A' = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
				-- 	and a.co_id = '02' 
				-- 	and b.co_id = '02'
				-- 	and b.status_id = 'A' 
				-- 	and 
				-- 	(
				-- 		case 
				-- 			when to_char('2024-01-01'::date,'yyyy') <= '2017' or TRUE is true
				-- 				then to_char(a.jv_date::date,'yyyy') <= to_char('2024-01-01'::date,'yyyy')
				-- 			else to_char(a.jv_date::date,'yyyy') = to_char('2024-01-01'::date,'yyyy')
				-- 		end
				-- 	) --add'l condition as per DCRF No. 453
				-- 	and (case when '' = '' then b.jv_no is not null else b.project_id = '' end)
				-- 	and (case when '' = '' then b.jv_no is not null else b.sub_projectid = '' end)
				-- 	and (case when NULL is null then true else a.period_id::int >= NULL end) 
				-- 	and (case when NULL is null then true else a.period_id::int <= NULL end) 	
				-- 	order by b.jv_no, b.entry_no  
				-- ) as a 

				-- join 
				-- (
				-- 	select distinct on (jv_no, co_id) jv_no, bal_side, sum(tran_amt) as debit, co_id   
				-- 	from rf_jv_detail where bal_side = 'D' and trim(acct_id) = '11-01-00-000'
				-- 	and co_id = '02' and status_id = 'A'  
				-- 	and (case when '' = '' then jv_no is not null else project_id = '' end)
				-- 	and (case when '' = '' then jv_no is not null else sub_projectid = '' end)
				-- 	group by jv_no, co_id, bal_side
				-- ) as c  
				-- on a.jv_no = c.jv_no and a.co_id = c.co_id 

				-- UNION ALL

				-- /*JV - CREDIT*/
				-- select -1 * (case when b.credit is null then 0 else b.credit end) as balance  
				-- from  			 
				-- (
				-- 	select distinct on (b.jv_no)  
				-- 	a.jv_date, b.entry_no, a.fiscal_yr,  
				-- 	a.period_id, b.tran_amt, b.jv_no, b.line_no, b.bal_side, a.status_id, b.co_id   
				-- 	from rf_jv_header a, rf_jv_detail b  
				-- 	where a.jv_no = b.jv_no and trim(b.acct_id) = '11-01-00-000'
				-- 	and a.jv_date::date < '2024-01-01'::date 
				-- 	and 
				-- 	(
				-- 		case 
				-- 		 	when a.fiscal_yr < to_char('2024-01-01'::date,'YYYY')::int 
				-- 		 		then true 
				-- 			else -- see DCRF No. 260
				-- 				(
				-- 					case 
				-- 						when '14' = '' 
				-- 							then a.period_id::int <= 12 
				-- 						else a.period_id::int <= coalesce('14',null)::int 
				-- 				 	end
				-- 				) 
				-- 		end
				-- 	)
				-- 	--and a.status_id ='P' 
				-- 	and (case when 'A' = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
				-- 	and a.co_id = '02' 
				-- 	and b.co_id = '02'
				-- 	and b.status_id = 'A' 
				-- 	and 
				-- 	(
				-- 		case 
				-- 			when to_char('2024-01-01'::date,'yyyy') <= '2017' or TRUE is true
				-- 				then to_char(a.jv_date::date,'yyyy') <= to_char('2024-01-01'::date,'yyyy')
				-- 			else to_char(a.jv_date::date,'yyyy') = to_char('2024-01-01'::date,'yyyy')
				-- 		end
				-- 	) --add'l condition as per DCRF No. 453
				-- 	and (case when '' = '' then b.jv_no is not null else b.project_id = '' end)
				-- 	and (case when '' = '' then b.jv_no is not null else b.sub_projectid = '' end)
				-- 	and (case when NULL is null then true else a.period_id::int >= NULL end) 
				-- 	and (case when NULL is null then true else a.period_id::int <= NULL end) 	
				-- 	order by b.jv_no, b.entry_no  
				-- ) as a 

				-- left join 
				-- (
				-- 	select distinct on (jv_no, co_id) jv_no, bal_side, sum(tran_amt) as credit, co_id  
				-- 	from rf_jv_detail where bal_side = 'C' and trim(acct_id) = '11-01-00-000'
				-- 	and co_id = '02' 
				-- 	and status_id = 'A'  
				-- 	and (case when '' = '' then jv_no is not null else project_id = '' end)
				-- 	and (case when '' = '' then jv_no is not null else sub_projectid = '' end)
				-- 	group by jv_no, co_id, bal_side
				-- ) as b  
				-- on a.jv_no = b.jv_no and a.co_id = b.co_id  

				-- UNION ALL 

				-- select 
				-- (case when c.debit is null then 0 else c.debit end) as balance  
				-- from  
				-- (
				-- 	select *
				-- 	from rf_cv_header a
				-- 	where status_id not in ('I','D')
				-- 	and a.date_paid::date < '2024-01-01'::date  --updated by Del G. 11/03/16
				-- 	and a.date_paid is not null --added by Del G. 11/03/16 
				-- 	and (case when 'A' = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
				-- 	and a.co_id = '02'
				-- 	and case when (EXTRACT(year from '2024-01-01'::TIMESTAMP) <= 2022 and date_paid::date > '2022-01-01'::date ) then true else (a.server_id is null OR remarks ~*'TRANSFERRED FROM ITS REAL') end -- added by erick 2023-09-22
				-- 	and cv_no NOT IN ('000060925')
				-- 	and 
				-- 		(
				-- 			case when to_char('2024-01-01'::date,'yyyy') <= '2017' or TRUE is true
				-- 			then
				-- 			to_char(a.date_paid::date,'yyyy') <= to_char('2024-01-01'::date,'yyyy')
				-- 			else
				-- 			to_char(a.date_paid::date,'yyyy') = to_char('2024-01-01'::date,'yyyy')
				-- 			end
				-- 		) --add'l condition as per DCRF No. 453
				-- ) as a  
				 
				-- join 
				-- (
				-- 	select distinct on (cv_no, co_id) acct_id, cv_no, bal_side, sum(tran_amt) as debit, co_id   
				-- 	from rf_cv_detail  
				-- 	where bal_side = 'D' 
				-- 	and trim(acct_id) = '11-01-00-000' 
				-- 	and co_id = '02' 
				-- 	and status_id = 'A' 
				-- 	group by cv_no, co_id, bal_side, acct_id
				-- ) as c 
				-- on a.cv_no = c.cv_no and a.co_id = c.co_id 
				-- where (case when '' = '' then a.cv_no is not null else c.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details)
				-- and (case when '' = '' then a.cv_no is not null else c.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details) 
				-- --and a.status_id ='P' 

				-- UNION ALL 

				-- /*CV - CREDIT*/
				-- select -1* (case when b.credit is null then 0 else b.credit end) as balance  
				-- from  
				-- (
				-- 	select *
				-- 	from rf_cv_header a
				-- 	where status_id not in ('I','D')
				-- 	and a.date_paid::date < '2024-01-01'::date  --updated by Del G. 11/03/16
				-- 	and a.date_paid is not null --added by Del G. 11/03/16 
				-- 	and (case when 'A' = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
				-- 	and a.co_id = '02'
				-- 	and case when (EXTRACT(year from '2024-01-01'::TIMESTAMP) <= 2022 and date_paid::date > '2022-01-01'::date ) then true else (a.server_id is null OR remarks ~*'TRANSFERRED FROM ITS REAL') end -- added by erick 2023-09-22
				-- 	and cv_no NOT IN ('000060925')
				-- 	and 
				-- 	(
				-- 		case 
				-- 			when to_char('2024-01-01'::date,'yyyy') <= '2017' or TRUE is true
				-- 				then to_char(a.date_paid::date,'yyyy') <= to_char('2024-01-01'::date,'yyyy')
				-- 			else to_char(a.date_paid::date,'yyyy') = to_char('2024-01-01'::date,'yyyy')
				-- 		end
				-- 	) --add'l condition as per DCRF No. 453
				-- ) as a  
				 
				-- join 
				-- (
				-- 	select distinct on (cv_no, co_id) acct_id, cv_no, bal_side, sum(tran_amt) as credit, co_id   
				-- 	from rf_cv_detail  
				-- 	where bal_side = 'C' 
				-- 	and trim(acct_id) = '11-01-00-000' 
				-- 	and co_id = '02' 
				-- 	and status_id = 'A' 
				-- 	group by cv_no, co_id, bal_side,acct_id
				-- ) as b   
				-- on a.cv_no = b.cv_no and a.co_id = b.co_id
				-- where (case when '' = '' then a.cv_no is not null else b.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details)
				-- and (case when '' = '' then a.cv_no is not null else b.acct_id = '00' end)  --this is included to remove all CV (since CV does not proj/subproj details) 
					 
				-- UNION ALL 

				-- /*PV - DEBIT*/
				-- select ( case when c.debit is null then 0 else c.debit end ) as balance  
				-- from  
				-- (
				-- 	select distinct on (a.pv_no) 
				-- 	a.pv_date, 
				-- 	b.tran_amt, 
				-- 	b.pv_no, 
				-- 	b.bal_side, 
				-- 	a.remarks, 
				-- 	a.status_id, 
				-- 	a.co_id, 
				-- 	b.project_id, 
				-- 	b.sub_projectid  
				-- 	from rf_pv_header a, rf_pv_detail b  
				-- 	where a.pv_no = b.pv_no 
				-- 	and trim(b.acct_id) = '11-01-00-000' 
				-- 	and a.pv_date::date < '2024-01-01'::date  
				-- 	and (case when 'A' = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
				-- 	and a.co_id = '02' and b.status_id = 'A' 
				-- 	and (case when '' = '' then b.pv_no is not null else b.project_id = '' end)
				-- 	and (case when '' = '' then b.pv_no is not null else b.sub_projectid = '' end)
				-- 	and 
				-- 	(
				-- 		case 
				-- 			when to_char('2024-01-01'::date,'yyyy') <= '2017' or TRUE is true
				-- 				then to_char(a.pv_date::date,'yyyy') <= to_char('2024-01-01'::date,'yyyy')
				-- 			else to_char(a.pv_date::date,'yyyy') = to_char('2024-01-01'::date,'yyyy')
				-- 		end
				-- 	) --add'l condition as per DCRF No. 453
				-- ) as a  
									 
				-- join 
				-- (
				-- 	select distinct on (pv_no, co_id) 
				-- 	pv_no, 
				-- 	bal_side, 
				-- 	sum(tran_amt) as debit, 
				-- 	co_id    
				-- 	from rf_pv_detail  
				-- 	where bal_side = 'D' 
				-- 	and trim(acct_id) = '11-01-00-000' 
				-- 	and co_id = '02' 
				-- 	and status_id = 'A' 
				-- 	and (case when '' = '' then pv_no is not null else project_id = '' end)
				-- 	and (case when '' = '' then pv_no is not null else sub_projectid = '' end)
				-- 	group by pv_no, co_id, bal_side
				-- ) as c  
				-- on a.pv_no = c.pv_no and a.co_id = c.co_id 

				-- UNION ALL 

				-- /*PV - CREDIT*/
				-- select -1*( case when b.credit is null then 0 else b.credit end ) as balance  
				-- from  
				-- (
				-- 	select distinct on (a.pv_no) a.pv_date, 
				-- 	b.tran_amt, 
				-- 	b.pv_no, 
				-- 	b.bal_side, 
				-- 	a.remarks, 
				-- 	a.status_id, 
				-- 	a.co_id, 
				-- 	b.project_id, 
				-- 	b.sub_projectid  
				-- 	from rf_pv_header a, 
				-- 	rf_pv_detail b  
				-- 	where a.pv_no = b.pv_no 
				-- 	and trim(b.acct_id) = '11-01-00-000' 
				-- 	and a.pv_date::date < '2024-01-01'::date  
				-- 	--and a.status_id ='P' 
				-- 	and (case when 'A' = 'A' then a.status_id not in ('I','D') else a.status_id = 'P' end) 
				-- 	and a.co_id = '02' and b.status_id = 'A' 
				-- 	and (case when '' = '' then b.pv_no is not null else b.project_id = '' end)
				-- 	and (case when '' = '' then b.pv_no is not null else b.sub_projectid = '' end)
				-- 	and 
				-- 	(
				-- 		case 
				-- 			when to_char('2024-01-01'::date,'yyyy') <= '2017' or TRUE is true
				-- 				then to_char(a.pv_date::date,'yyyy') <= to_char('2024-01-01'::date,'yyyy')
				-- 			else to_char(a.pv_date::date,'yyyy') = to_char('2024-01-01'::date,'yyyy')
				-- 		end
				-- 	) --add'l condition as per DCRF No. 453
				-- ) as a  

				-- left join 
				-- (
				-- 	select distinct on (pv_no, co_id) 
				-- 	pv_no, bal_side, sum(tran_amt) as credit, 
				-- 	co_id   
				-- 	from rf_pv_detail  	
				-- 	where bal_side = 'C' 
				-- 	and trim(acct_id) = '11-01-00-000' 
				-- 	and co_id = '02' 
				-- 	and status_id = 'A' 
				-- 	and (case when '' = '' then pv_no is not null else project_id = '' end)
				-- 	and (case when '' = '' then pv_no is not null else sub_projectid = '' end)	
				-- 	group by pv_no, co_id, bal_side
				-- ) as b  
				-- on a.pv_no = b.pv_no and a.co_id = b.co_id 

				-- UNION ALL 

				-- /*CRB*/
				select total as balance 
				from 
				(
					select 
					a.acct_id, 
					a.pay_rec_id, 
					sum(a.crb_amt) as total, 
					a.co_id
					from 
					(
						select  rb_id, doc_id, 
						acct_id, 
						pay_rec_id,
						 sum(trans_amt) as crb_amt,
						co_id
						from rf_crb_detail  
						where status_id = 'A' 
						and co_id = '02' 
						and trans_amt > 0
						and trim(acct_id) = '11-01-00-000'
						and nullif(TRIM(rb_id), '') IS NOT NULL
						and rb_fiscal_year < EXTRACT('YEAR' FROM '2024-01-01'::date)
						group by rb_id, co_id, doc_id, acct_id
						, pay_rec_id
					) a 
					LEFT join 
					(
						
						select  * 
						from 
							(
								select a.rb_id,a.issued_date,a.co_id,a.proj_id,a.phase,a.doc_id,a.remarks,a.status_id, a.pay_rec_id, a.reference_no  
								from rf_crb_header a
								where a.co_id='02'
								and (case when 'A' = 'A' then status_id IN ('A', 'P') else status_id = 'P' end)--//Comment by Erick 2019-06-26
								AND a.issued_date::DATE < '2024-01-01'
								--and (case when a.issued_date is null or to_char(a.issued_date,'yy')::int >= 18 then a.issued_date::date else a.issued_date::date end) < '2024-01-01'::date
								and nullif(TRIM(a.rb_id), '') IS NOT NULL
								and exists (SELECT * 
											FROM rf_crb_detail 
											where rb_id = a.rb_id
											and co_id = a.co_id
											and pay_rec_id = a.pay_rec_id
											and status_id = 'A'
											AND acct_id = '11-01-00-000')
							)a
						where  
						--status_id not in ('I','D') 
						--AND 
						NOT EXISTS (select * from issued_garbage_fee where client_seqno = a.reference_no) -- to exclude payments from happywell
						--and (case when 'A' = 'A' then true else status_id = 'P' end)--//Comment by Erick 2019-06-26 	
						and (rb_id, doc_id, pay_rec_id::int) not in  
						(

							select coalesce(si_no, or_no), (case when or_date is NOT null then '01' when si_date is not null then '307' else '03' end), pay_rec_id
							from rf_payments 
							where  
								(
								case when to_char('2024-01-01'::date,'yyyy')::int <= 2017
								--case when '2024-01-01'::date <= '2016-10-26'
									then (remarks like '%JV No.%' and remarks like '%Late%' and to_char((case when coalesce(si_date, or_date) is null then trans_date else coalesce(si_date,or_date) end),'yyyy')::int >= 2017)  
							else ( remarks like '%Late%' and to_char((case when coalesce(si_date, or_date) is null then trans_date else coalesce(si_date, or_date) end),'yyyy')::int >= 2017)
								end 
								)/*ADDED BY ERICK BITUEN DATED 2019-08-08-- TO INCLUDE LATE OR  W/O JV IN THE PERIOD TRANSACTION*/
							and co_id = '02' --2022-06-13
							union all

							/*	Added by: Mann2x; Date Added: September 11, 2018; As requested by Hazel that the payments made through client requests should not be included in the GL;	*/
							select coalesce(si_no, or_no), (case when or_date is NOT null then '01' when si_date is not null then '307' else '03' end) , pay_rec_id
							from rf_payments 
							where remarks like '%Special Case%' 
							/*	Modified by Mann2x; Date Modified: January 17; Every payments credited from client requests should not be included;	*/
							--and trans_date::date >= '2024-01-01'::date
							and trans_date::date >= '2018-01-01'::date
							and co_id = '02' --added by lester to filter receipt from different companies 2022-03-23
							--and server_id IS NULL
							union all

							select ar_no, '03', pay_rec_id 
							from rf_payments 
							where --status_id='A' --comment by Erick Bituen dated 10-01-2020
							(status_id = 'A' or (status_id = 'I' and (request_no is not null or coalesce(request_no, '') != '' or request_no != '') and (remarks like '%Special Case Credit of Payment%' or remarks ~* 'Special Case')) ) --added by Erick Bituen 10-01-2020 to include client request with inactive status
							and (request_no is not null and refund_date is null)  and to_char(trans_date,'yyyy')::int >= 2018 and ar_no is not null --added by Erick 2019-07-15 dcrf 1121
							and co_id = '02'
							union all
							
							--Added by Erick dated 2021-06-04 another special case reference DCRF 1676, 1678, 1679, 1681
							select or_no, '01', pay_rec_id
							from rf_payments
							where pay_rec_id in ('80684', '80693', '80685', '80695', '80687', '80694', '80686', '81160', '81161')
							and co_id = '02' --2022-06-13
						) 
						and case when '02' = '01' then a.rb_id not in ('005987','005988','006018B','006019B','006035B','006036B','006037B') else true end -- Added by Erick 2023-11-22 to exclude in GL Hernandez Karen payment
						-- added condition as per DCRF No. 429			
					) b on a.rb_id = b.rb_id and a.doc_id = b.doc_id and a.pay_rec_id::int = b.pay_rec_id::int and a.co_id = b.co_id
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
							and co_id = '02' 
							and server_id IS NULL
						) a 
					) c on a.rb_id = c.or_no and a.doc_id = c.doc_id and a.pay_rec_id::int = c.pay_rec_id::int --added by DG ; 03/13/2017
					where (case when c.or_date is null or to_char(c.or_date,'yy')::int >= 18 then b.issued_date::date else c.or_date::date end) < '2024-01-01'::date  --updated by Del G. on 11-07-2016 for Late LTS OR
					and (case when '' = '' then a.rb_id is not null else b.proj_id= '' end)
					and (case when '' = '' then a.rb_id is not null else b.phase = '' end)
					and 
					(
						case 
							when to_char('2024-01-01'::date,'yyyy') <= '2017' or TRUE is true
								then to_char
								(
									(
										case 
											when c.or_date is null or to_char(or_date,'yy')::int >= 18 
												then b.issued_date::date 
											else c.or_date::date 
										end),'yyyy'
									) <= to_char('2024-01-01'::date,'yyyy')
							else to_char
								(
									(
										case 
											when c.or_date is null or to_char(or_date,'yy')::int >= 18 
												then b.issued_date::date 
											else c.or_date::date 
										end
									),'yyyy') = to_char('2024-01-01'::date,'yyyy')
						end
					) --add'l condition as per DCRF No. 453
					group by a.acct_id, a.co_id, a.pay_rec_id
				) a  
			) a  

			--order by a.gl_date::date
		) z