-- FUNCTION: public.view_ewt_forremittance_all_v2_debug(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying)

-- DROP FUNCTION IF EXISTS public.view_ewt_forremittance_all_v2_debug(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION public.view_ewt_forremittance_all_v2_debug(
	p_co_id character varying,
	p_payee_id character varying,
	p_rplf_no character varying,
	p_pv_no character varying,
	p_jv_no character varying,
	p_year character varying,
	p_period character varying,
	p_period1 character varying,
	p_period2 character varying,
	p_period3 character varying,
	p_month character varying,
	p_acct_id character varying,
	p_entity_type_id character varying,
	p_emp_code character varying)
    RETURNS TABLE(c_tag boolean, c_entity_id2 character varying, c_tin_no character varying, c_client character varying, c_rplf_no character varying, c_cv_no character varying, c_jv_no character varying, c_pv_date character varying, c_wtax_amt numeric, c_net_paid numeric, c_date_paid character varying, c_with_lts boolean, c_retper character varying, c_bircode character varying, c_income_payment_desc character varying, c_taxrate numeric, c_acct_name character varying, c_sub_projectid character varying, c_first_day date, c_last_day date, c_phase character varying, c_project character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

v_rec RECORD;
v_first_day date; 
v_last_day date; 

BEGIN

	RAISE INFO ''; 

	RAISE INFO 'p_co_id: %', p_co_id; 
	RAISE INFO 'p_payee_id: %', p_payee_id; 
	RAISE INFO 'p_rplf_no: %', p_rplf_no; 
	RAISE INFO 'p_pv_no: %', p_pv_no; 
	RAISE INFO 'p_jv_no: %', p_jv_no; 
	RAISE INFO 'p_year: %', p_year; 
	RAISE INFO 'p_period: %', p_period; 
	RAISE INFO 'p_period1: %', p_period1; 
	RAISE INFO 'p_period2: %', p_period2; 
	RAISE INFO 'p_period3: %', p_period3; 
	RAISE INFO 'p_month: %', p_month; 
	RAISE INFO 'p_acct_id: %', p_acct_id; 
	RAISE INFO 'p_entity_type_id: %', p_entity_type_id; 
	
	--v_first_day	:= 
	
	CREATE TEMP TABLE tmp_ewt_liq AS
	select MAX(co_id) as co_id,MAX(wtax_id) as wtax_id,MAX(entity_type_id) as entity_type_id, liq_no from rf_liq_detail ldet
	where ldet.co_id = p_co_id
	and ldet.status_id != 'I'
	and ldet.wtax_amt > 0
	group by liq_no;
	
	delete from tmp_EWT_forRemittance_all_v2 where emp_code = p_emp_code; 
	insert into tmp_EWT_forRemittance_all_v2
	
	select false as tag, *, p_emp_code, 
	
	(
		case 
			when p_month = ''
				then null
			else concat(p_month, '-', '01', '-', date_part('year', now()))
		end
	)::date as first_date, 
	(
		case
			when p_month = ''
				then null
			else date_trunc('month', concat(p_month, '-', '01', '-', date_part('year', now()))::timestamp) + interval '1 month' - interval '1 day'
		end
	)::date as last_date
	
	from 
	(
		select 
		(
			case 
				when coalesce(NULLIF(TRIM(g.jv_no), ''), '') != '' 
					then g.entity_id 
				else (case when trim(b.rplf_type_id) = '04' then (a.entity_id) else trim(bb.entity_id2) end) 
			end
		) as entity_id2,
		cc.tin_no,
		upper(trim(c.entity_name)) as client,
		a.rplf_no,
		bb.cv_no,
		g.jv_no as jv_no,
		to_char(bb.pv_date,'MM-dd-yyyy') as pv_date,
		(case when coalesce(g.jv_no, '') <> '' then g.wtax_amt else a.wtax_amt end) as wtax_amt,

		/*	RIDER CHANGE	*/
		(
			case
				when coalesce(g.jv_no, '') <> ''
					then j.net_paid
				else 
				(
					case
						when coalesce(h.wtax_rate, 0) = 0
							then null 
						else
							--case when a.vat_amt != 0 then 
								a.exp_amt --DEFAULT BY LESTER TO EXP AMOUNT BECAUSE OF WRONG COMPUTATION FOR DECIMAL PLACES
							--else --REPLACE THIS WHEN WRONG COMPUTATION FOR NET AMOUNT
						       --(a.wtax_amt /  (ROUND(h.wtax_rate::DECIMAL, 2) / 100))::numeric(19, 2) --replace with amount because of wrong computation of amount
							--end
					end
				)

			end
		) as net_paid, 
		
		to_char(e.date_paid,'MM-dd-yyyy') as date_paid, 
		(case when a.sub_projectid is not null or a.sub_projectid = '' then true else (case when f.lts_date is null then false else true end) end) as with_lts,
		--concat_ws('/', LPAD(DATE_PART('Month', b.rplf_date)::CHAR(2), 2, '0'), 	RIGHT(DATE_PART('YEAR', b.rplf_date)::CHAR(4), 2)) as RetPer, 
		concat_ws('/', LPAD(DATE_PART('Month', bb.pv_date)::CHAR(2), 2, '0'), 	RIGHT(DATE_PART('YEAR', bb.pv_date)::CHAR(4), 2)) as RetPer, --DCRF 3138 
		h.wtax_bir_code as bircode, h.income_payment_desc, ROUND(h.wtax_rate::DECIMAL, 2) as tax_rate, 
		i.acct_name, 
		coalesce(concat('Phase',(select phase from mf_sub_project where sub_proj_id = a.sub_projectid and proj_id = a.project_id and status_id != 'I'),'')) as phase,
		(select proj_name from mf_project where proj_id = a.project_id) as proj_name
		from 
		(
			select * 
			from rf_request_detail  x
			where x.status_id != 'I'
			and x.wtax_amt != 0
			and x.co_id = p_co_id
			/*COMMENTED BY JED 2021-06-16 : EWT FOR COMMISSION ARE INCLUDED*/
			/**Temporary Condition*/
-- 			and not exists
-- 			(
-- 				/**For Tax Refund Purposes Only**/
-- 				select coalesce(y.ref_no,''), y.co_id
-- 				from 
-- 				(
-- 					select ii.ref_no, a.co_id
-- 					from (select * from rf_request_detail where status_id = 'A') a
-- 					join 
-- 					(
-- 						select * 
-- 						from rf_request_header x
-- 						where x.rplf_type_id = '04'
-- 						and not exists (select * from cm_cdf_dl y where y.wtax_amt < 0 and y.ref_no = x.rplf_no and x.co_id = y.co_id) 
-- 					) b on a.rplf_no =b.rplf_no and a.co_id = b.co_id
-- 					left join rf_pv_header d on b.rplf_no = d.rplf_no and b.co_id = d.co_id
-- 					left join rf_cv_header e on d.cv_no = e.cv_no and d.co_id = e.co_id	
-- 					left join 
-- 					(
-- 						select distinct on (pbl_id, seq_no, comm_type, a.cdf_no) pbl_id, seq_no, comm_type, a.cdf_no, ref_no, 
-- 						applied_amt, vat_amt, a.wtax_amt, caliq_amt, agent_code  
-- 						from cm_cdf_dl a
-- 						left join cm_cdf_hd b on a.cdf_no = b.cdf_no
-- 					) i on b.rplf_no = i.ref_no and a.pv_amt = i.applied_amt
-- 					left join 
-- 					(
-- 						select distinct on (pbl_id, seq_no, comm_type, a.cdf_no) 
-- 						pbl_id, seq_no, comm_type, a.cdf_no, ref_no, 
-- 						applied_amt, vat_amt, a.wtax_amt, caliq_amt, agent_code 
-- 						from cm_cdf_dl a
-- 						left join (select * from cm_cdf_hd where status_id != 'I') b on a.cdf_no = b.cdf_no
-- 					) ii on i.pbl_id = ii.pbl_id and i.seq_no = ii.seq_no and i.comm_type = ii.comm_type and i.cdf_no != ii.cdf_no and i.agent_code = ii.agent_code				
-- 					where b.rplf_date >= '2014-01-01 00:00:00'
-- 					and a.co_id = '02'
-- 					and d.cv_no is not null
-- 				) y
-- 				where coalesce(y.ref_no,'') = x.rplf_no and x.co_id = y.co_id
-- 			)
			
			/**Temporary Condition*/	
			/*COMMENTED BY JED 2021-06-16 : EWT FOR COMMISSION ARE INCLUDED*/
		) a
		left join mf_project mfp on a.co_id = mfp.co_id and a.project_id = mfp.proj_id
		--left join (select * from rf_request_header where status_id != 'I') b on a.rplf_no =b.rplf_no and a.co_id = b.co_id
		--left join (select * from rf_request_header where status_id != 'I' and rplf_type_id != '02') b on a.rplf_no =b.rplf_no and a.co_id = b.co_id
		left join (select * from rf_request_header where status_id = 'A') b on a.rplf_no =b.rplf_no and a.co_id = b.co_id
		--left join (select * from rf_pv_header where status_id != 'I') bb on a.rplf_no =bb.rplf_no and a.co_id = bb.co_id
		left join (select * from rf_pv_header where status_id = 'P') bb on a.rplf_no =bb.rplf_no and a.co_id = bb.co_id

		/*added by Del Gonzales : 03/07/2017*/
		left join (select * from mf_entity_type where status_id != 'I') ab on bb.entity_type_id = ab.entity_type_id 
		/*added by Del Gonzales : 03/07/2017*/
		
		left join (select * from rf_cv_header where status_id = 'P') e on bb.cv_no = e.cv_no and bb.co_id = e.co_id
		and coalesce(e.server_id, '') = coalesce(mfp.server_id, '')
		left join (select * from mf_sub_project where status_id != 'I') f on a.sub_projectid = f.sub_proj_id and a.project_id = f.proj_id
		and coalesce(f.server_id, '') = coalesce(mfP.server_id, '')
		left join 
		(
			select y.rplf_no, z.entity_id, z.liq_no, z.rplf_line_no, z.wtax_amt, z.wtax_id, z.entity_type_id, x.*
			from rf_jv_header x 
			left join (select * from rf_liq_header where status_id != 'I') y on x.jv_no = y.jv_no and x.co_id = y.co_id
			right join (select * from rf_liq_detail where status_id != 'I') z on y.liq_no = z.liq_no and x.co_id = y.co_id
			--where x.status_id != 'I'	
			where x.status_id = 'P'
			
			order by y.rplf_no
		) g on b.rplf_no = g.rplf_no and b.co_id = g.co_id and a.line_no = g.rplf_line_no
		left join rf_entity c on 
		--(case when coalesce(g.jv_no, '') != '' then b.entity_id1 else (case when trim(b.rplf_type_id) = '04' then (a.entity_id) else trim(bb.entity_id2) end) end) = c.entity_id

		(
			case 
				when coalesce(g.jv_no, '') != '' 
					--then g.entity_id 
					then b.entity_id1
				else 
-- 					(
-- 						case 
-- 							when trim(b.rplf_type_id) = '04' 
-- 								then (a.entity_id) 
-- 							else trim(bb.entity_id2) 
-- 						end
-- 					) 
			
				trim(a.entity_id) --DCRF 3138
			end
		) = c.entity_id
-- 		and coalesce(c.server_id, '') = coalesce(mfp.server_id, '')
		
		--left join rf_entity_id_no cc on (case when coalesce(g.jv_no, '') != '' then g.entity_id else (case when trim(b.rplf_type_id) = '04' then (a.entity_id) else trim(bb.entity_id2) end) end) = cc.entity_id 
		left join rf_entity_id_no cc on (case when coalesce(g.jv_no, '') != '' then b.entity_id1 else (case when trim(b.rplf_type_id) = '04' then (a.entity_id) else trim(bb.entity_id2) end) end) = cc.entity_id 
		LEFT JOIN rf_withholding_tax h ON 
		(
			CASE
				WHEN COALESCE(g.jv_no, '') = ''	
					THEN coalesce(a.wtax_id,ab.wtax_id)
				ELSE  g.wtax_id
			END
		) = h.wtax_id 
		left join mf_boi_chart_of_accounts i on i.acct_id = a.acct_id
		left join
		(
				select sum(x.tran_amt - x.vat_amt) as net_paid, y.jv_no, x.entity_type_id
				from rf_liq_detail x
				inner join rf_liq_header y on y.liq_no = x.liq_no and x.co_id = y.co_id
				where x.status_id = 'A'
				group by y.jv_no, x.entity_type_id
		) j on j.jv_no = g.jv_no and j.entity_type_id = g.entity_type_id
		where coalesce(g.jv_no, '') = ''
		and bb.pv_date >= '2014-01-01 00:00:00' 
		and a.co_id = p_co_id 

		-- Modified by Mann2x; Date Modified: February 13, 2017; The date filter for JV numbers shoud be the date from the liquidation;
		-- and trim(to_char(bb.pv_date, 'yyyy')) = p_year
		and 
		(
			case
				when coalesce(g.jv_no, '') <> ''
					then trim(to_char(g.jv_date, 'yyyy'))
				else trim(to_char(bb.pv_date, 'yyyy'))
			end
		) = p_year

		and (case when p_payee_id = '' then true else (case when trim(b.rplf_type_id) = '04' then (a.entity_id) else trim(b.entity_id1) end) = p_payee_id end)
		and (case when p_rplf_no = '' then true else b.rplf_no = p_rplf_no end)
		and (case when p_jv_no = '' then true else b.rplf_no is null end)
		
		-- Modified by Mann2x; Date Modified: February 13, 2017; The date filter for JV numbers shoud be the date from the liquidation;
		-- and (case when p_year = '' then true else trim(to_char(bb.pv_date, 'yyyy')) = p_year end)
		and 
		(
			case
				when coalesce(g.jv_no, '') <> ''
					then (case when p_year = '' then true else trim(to_char(g.jv_date, 'yyyy')) = p_year end)
				else (case when p_year = '' then true else trim(to_char(bb.pv_date, 'yyyy')) = p_year end)
			end
		)

		-- Modified by Mann2x; Date Modified: February 13, 2017; The date filter for JV numbers shoud be the date from the liquidation;
		-- and (case when p_period = 'All' then true else substr(trim(to_char(bb.pv_date, 'MM-dd-yyyy')),0,3) in (p_period1,p_period2,p_period3) end)
		-- and (case when p_month = '' then true else substr(trim(to_char(bb.pv_date, 'MM-dd-yyyy')),0,3) = p_month end)

		and 
		(
			case
				when coalesce(g.jv_no, '') <> ''
					then (case when p_period = 'All' then true else substr(trim(to_char(g.jv_date, 'MM-dd-yyyy')), 0, 3) in (p_period1, p_period2, p_period3) end)
				else (case when p_period = 'All' then true else substr(trim(to_char(bb.pv_date, 'MM-dd-yyyy')), 0, 3) in (p_period1, p_period2, p_period3) end)
			end
		)
		and 
		(
			case
				when coalesce(g.jv_no, '') <> ''
					then (case when p_month = '' then true else substr(trim(to_char(g.jv_date, 'MM-dd-yyyy')), 0, 3) = p_month end)
				else (case when p_month = '' then true else substr(trim(to_char(bb.pv_date, 'MM-dd-yyyy')), 0, 3) = p_month end)
			end
		)
		and (case when p_pv_no = '' then true else a.rplf_no = p_pv_no end)
		and (case when p_acct_id = '' then true else a.acct_id like p_acct_id end)
		and (case when p_entity_type_id = '' then true else b.entity_type_id = p_entity_type_id end)
		and exists (
					select 
					--sum(x.tran_amt) as wtax_amt,
					x.pv_no,
					x.co_id,
					x.tran_amt
					from rf_pv_detail x
					where x.pv_no = a.rplf_no
					and x.co_id = a.co_id 
					and x.status_id = 'A' 
					and x.acct_id = '03-01-06-002'
					and x.bal_side = 'C'
					--and x.pv_no = '000018557'
					group by x.co_id, x.pv_no, x.tran_amt
				)

		/*** FOR COMMISSION TAX ADJUSTMENT PURPOSES ONLY ***/
		UNION ALL

		--✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮
		--✿⊱╮	Added by Mann2x; Date Added: July 5, 2019; DCRF# 1062;	 ♥✿⊱╮
		--✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮
		select a.entity_id as entity_id2,
		CASE WHEN EXISTS (SELECT * FROM rf_liq_header where rplf_no = a.rplf_no and co_id = a.co_id) then (select tin_no from rf_entity_id_no where entity_id = a.entity_id and status_id = 'A') ELSE c.tin_no END as tin_no,
		CASE WHEN EXISTS (SELECT * FROM rf_liq_header where rplf_no = a.rplf_no and co_id = a.co_id) then get_client_name(a.entity_id) else d.entity_name end as client,
		--'test' as client,--d.entity_name as client,
		a.rplf_no,
		e.cv_no,
		a.jv_no,
		to_char(coalesce(e.pv_date,a.jv_date), 'MM-dd-yyyy') as pv_date, 
		a.wtax_amt,
		a.net_paid,
		to_char((f.date_paid), 'MM-dd-yyyy') as date_paid,
		g.lts_date is not null as with_lts,
		concat_ws('/', LPAD(DATE_PART('Month', b.rplf_date)::CHAR(2), 2, '0'), RIGHT(DATE_PART('YEAR', b.rplf_date)::CHAR(4), 2)) as RetPer,
		h.wtax_bir_code as bircode,
		h.income_payment_desc,
		h.wtax_rate::numeric(19, 2) as tax_rate, 
		i.acct_name, 
		coalesce(concat('Phase',(select phase from mf_sub_project where sub_proj_id = a.sub_projectid and proj_id = a.proj_id and status_id != 'I'),'')) as phase,
		(select proj_name from mf_project where proj_id = a.proj_id) as proj_name
		from
		(
			select *
			from
			(
				/*	RIDER CHANGE	*/
				select y.rplf_no,
				z.entity_id,
				z.liq_no,
				z.rplf_line_no,
				z.wtax_amt, 
				z.wtax_id,
				z.entity_type_id, 
				/*z.tran_amt-z.wtax_amt as net_paid, changed by jari cruz asof sept 9 2022 reason mali ung pinagbabawasan dapat vat daw*/
				z.tran_amt-z.vat_amt as net_paid,
				x.co_id, 
				x.jv_no, z.acct_id, x.jv_date, z.sub_projectid, z.project_id as proj_id
				from (select * from rf_jv_header where status_id = 'P' AND co_id = p_co_id) x 
				left join (select * from rf_liq_header where status_id != 'I') y on x.jv_no = y.jv_no and x.co_id = y.co_id
				left join (select * from rf_liq_detail where status_id != 'I') z on y.liq_no = z.liq_no and z.co_id = y.co_id and z.busunit_id = y.busunit_id
				-- where not exists (select * from rf_subsidiary_ledger xx where xx.jv_no = x.jv_no and xx.status_id = 'A') modified by jari cruz asof april 14 2023
				where not exists (select * from rf_subsidiary_ledger xx where xx.jv_no = x.jv_no and xx.co_id = x.co_id and xx.status_id = 'A')
				union 
				
				select 
				y.rplf_no, 
				x.entity_id, 
				y.liq_no, 
				row_number() over() as rplf_line_no, 
				x.wtax_amt, 
				/*xx.wtax_id,
				commented by jari cruz asof oct 7 2022 reason null value if walang laman ung nasa rf_liq_detail*/
-- 				(case 
-- 				 when nullif(trim(xx.wtax_id),'') is null then (select wtax_id from mf_entity_type where entity_type_id = xx.entity_type_id)
-- 				else xx.wtax_id end) as wtax_id, 
				coalesce(xx.wtax_id, b.wtax_id) as wtax_id,
				xx.entity_type_id as entity_type_id, 
-- 				(SELECT case when nullif(trim(a.wtax_id),'') is null then (select wtax_id from mf_entity_type where entity_type_id = a.entity_type_id) else wtax_id end FROM rf_liq_detail a where a.status_id = 'A' and a.wtax_amt > 0 and a.liq_no = y.liq_no and a.co_id = y.co_id LIMIT 1) as wtax_id, 
-- 				(SELECT entity_type_id FROM rf_liq_detail a where a.status_id = 'A' and a.wtax_amt > 0 and a.liq_no = y.liq_no and a.co_id = y.co_id LIMIT 1) AS  entity_type_id,
				/*x.tran_amt-x.wtax_amt as net_paid, changed by jari cruz asof Nov 7 2022 reason mali ung pinagbabawasan dapat vat daw*/
				x.trans_amt-x.vat_amt as net_paid,
				z.co_id, x.jv_no, x.sundry_acct, z.jv_date, x.sub_proj as sub_projectid, x.proj_id
				from rf_subsidiary_ledger x
				left join (select * from rf_liq_header where status_id != 'I') y on x.jv_no = y.jv_no and x.co_id = y.co_id
				inner join rf_jv_header z on x.jv_no = z.jv_no and x.co_id = z.co_id
				/*
				commented by jari cruz asof august 5 2022 reason doubling results or worse 4x it.
				left join
				(
					select wtax_id, entity_type_id, liq_no, co_id
					from rf_liq_detail
					group by wtax_id, entity_type_id, liq_no, co_id
				) xx on xx.liq_no = y.liq_no and xx.co_id = y.co_id*/
				left join tmp_ewt_liq xx on xx.liq_no = y.liq_no and xx.co_id = y.co_id
				LEFT JOIN mf_entity_type a on x.entity_type_id = a.entity_type_id
				left join rf_withholding_tax b on b.wtax_id = a.wtax_id
-- 				and xx.tran_amt = x.trans_amt -- added by jari cruz asof sept 15 2022, reason for filter
				where x.status_id = 'A'
				and x.co_id = p_co_id
				/*	RIDER CHANGE	*/
			) x
			where x.wtax_amt > 0 -- added by jari cruz asof august 4 2022
			order by x.rplf_no, x.rplf_line_no
		) a
		left join mf_project mfpp on a.co_id = mfpp.co_id and a.proj_id = mfpp.proj_id 
		left join (select * from rf_request_header where status_id != 'I' ) b on a.rplf_no = b.rplf_no and a.co_id = b.co_id
		--left join (select * from rf_request_header where status_id != 'I' and rplf_type_id != '02' ) b on a.rplf_no = b.rplf_no and a.co_id = b.co_id
		left join (select * from rf_entity_id_no where status_id = 'A') c on coalesce(b.entity_id1, a.entity_id) = c.entity_id
		--left join rf_entity d on b.entity_id1 = d.entity_id /*commented by jed 2022-02-07 for manual jv to get payee name thru subsidiary ledger*/
		left join rf_entity d on coalesce(b.entity_id1, a.entity_id) = d.entity_id 
-- 		and coalesce(d.server_id, '') = coalesce(mfpp.server_id, '')
		--left join (select * from rf_pv_header where status_id != 'I') e on a.rplf_no = e.rplf_no
		left join (select * from rf_pv_header where status_id = 'P') e on a.rplf_no = e.rplf_no
		and a.co_id = e.co_id -- added by jari asof sept 13 2022 reason rplf is not unique if no company
		left join (select * from rf_cv_header where status_id = 'P') f on e.cv_no = f.cv_no and e.co_id = f.co_id
		and coalesce(f.server_id, '') = coalesce(mfpp.server_id, '')
		left join (select * from mf_sub_project where status_id != 'I') g on (select x.sub_projectid from rf_request_detail x where x.rplf_no = b.rplf_no and x.co_id = b.co_id and x.status_id != 'I' limit 1) = g.sub_proj_id
		and coalesce(g.server_id, '') = coalesce(mfpp.server_id, '')
		LEFT JOIN rf_withholding_tax h ON h.wtax_id = a.wtax_id
		left join mf_boi_chart_of_accounts i on i.acct_id = a.acct_id
		where (case when e.pv_date is null then true else e.pv_date::date >= '2014-01-01'::date end)
		and (a.co_id = p_co_id or p_co_id = '')
		and date_part('year', a.jv_date)::varchar = p_year
		
		and (b.entity_id1 = p_payee_id or p_payee_id = '')
		
		and (b.rplf_no = p_rplf_no or p_rplf_no = '')
		and (a.jv_no = p_jv_no or p_jv_no = '')
		and (CASE WHEN p_month = '' THEN LPAD(date_part('month', a.jv_date)::text, 2, '0') in (p_period1, p_period2, p_period3)
			 ELSE LPAD(date_part('month', a.jv_date)::text, 2, '0') = p_month END)
 		/*
		and LPAD(date_part('month', a.jv_date)::text, 2, '0') = p_month
		commented by jari cruz asof oct 24 2022
		reason, di lumalabas jv pag naka ALL
		*/
		and (a.acct_id = p_acct_id or p_acct_id = '')
		and (b.entity_type_id = p_entity_type_id or p_entity_type_id = '')
		
		UNION ALL
		--✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮
		--✿⊱╮	Added by Mann2x; Date Added: July 5, 2019; DCRF# 1062;	 ♥✿⊱╮
		--✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮♥✿⊱╮
		
		/**For Tax Refund Purposes Only**/
		select distinct on (rplf_no) entity_id1, tin_no, client, rplf_no, cv_no, null as jv_no, to_char(pv_date,'MM-dd-yyyy') as pv_date,
		sum(wtax_amt) as  wtax_amt, sum(exp_amt) as exp_amt, null as date_paid, true as with_lts,
		concat_ws('/', LPAD(DATE_PART('Month', rplf_date)::CHAR(2), 2, '0'), RIGHT(DATE_PART('YEAR', rplf_date)::CHAR(4), 2)) as RetPer, 
		bircode, income_payment_desc, tax_rate, 'Advances to Brokers - Commission Advance' as acct_name, 
		coalesce(concat('Phase',(select phase from mf_sub_project where sub_proj_id = a.sub_projectid and proj_id = a.proj_id and status_id != 'I'),'')) as phase,
		(select proj_name from mf_project where proj_id = a.proj_id) as proj_name
		from 
		(
			select b.entity_id1, 
			upper(trim(c.entity_name)) as client, 
			a.rplf_no, d.cv_no, b.rplf_date, d.pv_date,
			(ii.wtax_amt - a.exp_amt) as wtax_amt, (ii.applied_amt + ii.caliq_amt + ii.wtax_amt) as exp_amt,
			trim(to_char(b.rplf_date, 'MM')) as period, b.rplf_type_id, replace(replace(f.tin_no,'-',''),' ','') as tin,
			upper(get_client_address_for2307(b.entity_id1)) as address, a.co_id,
			(case when a.co_id = '02' then '1212121212' else '' end) as co_entity,
			a.wtax_id, '1'::text as x, j.tin_no, l.wtax_bir_code as bircode, l.income_payment_desc,
			ROUND(l.wtax_rate::DECIMAL, 2) as tax_rate, (select x.acct_name from mf_boi_chart_of_accounts x where x.acct_id = a.acct_id) as acct_name, 
			a.sub_projectid, a.project_id as proj_id
			from (select * from rf_request_detail where status_id = 'A') a
			left join mf_project mfppp on a.co_id = mfppp.co_id and a.project_id = mfppp.proj_id
			join 
			(
				select * 
				from rf_request_header 
				where rplf_type_id = '04'
				and rplf_no in (select ref_no from cm_cdf_dl where wtax_amt < 0) 
				and status_id = 'A'
			) b on a.rplf_no = b.rplf_no and a.co_id = b.co_id
			left join rf_entity c on b.entity_id1 = c.entity_id
-- 			and coalesce(c.server_id, '') = coalesce(mfppp.server_id, '')
			left join rf_pv_header d on b.rplf_no = d.rplf_no and b.co_id = d.co_id
			left join rf_cv_header e on d.cv_no = e.cv_no and d.co_id = e.co_id
-- 			and coalesce(e.server_id, '') = coalesce(mfppp.server_id, '')
			left join rf_entity_id_no f on b.entity_id1 = f.entity_id
			left join 
			(
				select distinct on (entity_id) * 
				from rf_entity_address 
				--where entity_id = '4738443684'
				order by entity_id, rec_id desc
			) g on a.entity_id = g.entity_id
			left join mf_city h on g.city_id = h.city_id
			left join 
			(
				select distinct on (pbl_id, seq_no, comm_type, a.cdf_no) pbl_id, seq_no, comm_type, a.cdf_no, ref_no, 
				applied_amt, vat_amt, a.wtax_amt, caliq_amt, agent_code  
				from cm_cdf_dl a
				left join cm_cdf_hd b on a.cdf_no = b.cdf_no
				--where ref_no = '000009420'
			) i on b.rplf_no = i.ref_no and a.pv_amt = i.applied_amt
			left join 
			(
				select distinct on (pbl_id, seq_no, comm_type, a.cdf_no) 
				pbl_id, seq_no, comm_type, a.cdf_no, ref_no, 
				applied_amt, vat_amt, a.wtax_amt, caliq_amt, agent_code 
				from cm_cdf_dl a
				left join (select * from cm_cdf_hd where status_id != 'I') b on a.cdf_no = b.cdf_no
				--where a.cdf_no::int = 3721
			) ii on i.pbl_id = ii.pbl_id and i.seq_no = ii.seq_no and i.comm_type = ii.comm_type and i.cdf_no != ii.cdf_no and i.agent_code = ii.agent_code
			left join rf_entity_id_no j on b.entity_id1 = j.entity_id
			left join (select * from mf_entity_type where status_id != 'I') k on d.entity_type_id = k.entity_type_id 	
			LEFT JOIN rf_withholding_tax l on k.wtax_id = l.wtax_id 
			where b.rplf_date >= '2014-01-01 00:00:00' 
			and a.co_id = p_co_id 
			and d.cv_no is not null
			/*ADDED BY JED 2021-06-16 : ADD ADDITIONAL FILTER TO GET PRESENT YEAR ONLY*/
			and trim(to_char(d.pv_date, 'yyyy')) = p_year
			and (case when p_year = '' then true else trim(to_char(d.pv_date, 'yyyy')) = p_year end)
			and (case when p_period = 'All' then true else substr(trim(to_char(d.pv_date, 'MM-dd-yyyy')), 0, 3) in (p_period1, p_period2, p_period3) end)
			and (case when p_month = '' then true else substr(trim(to_char(d.pv_date, 'MM-dd-yyyy')), 0, 3) = p_month end)
			--and b.entity_id1 = '4245292623'
			/*ADDED BY JED 2021-06-16 : ADD ADDITIONAL FILTER TO GET PRESENT YEAR ONLY*/
		) a

		group by sub_projectid, a.proj_id, entity_id1, client,  rplf_no,
		cv_no, rplf_date, pv_date,
		period,	rplf_type_id,	tin,
		address, co_id,	co_entity, wtax_id, 
		x, tin_no, bircode, income_payment_desc,
		tax_rate, acct_name
		/*** FOR COMMISSION TAX ADJUSTMENT PURPOSES ONLY ***/
	) a 
	where a.rplf_no not in (select rplf_no from rf_ewt_remittance) 
	and a.wtax_amt > 0 
	order by a.client, a.rplf_no; 
	
	DROP TABLE tmp_ewt_liq;
	
	FOR v_rec IN 
	(
		select *
		from tmp_EWT_forRemittance_all_v2 
		where emp_code = p_emp_code
		order by c_client, c_rplf_no
	) LOOP

		c_tag			:= v_rec.c_tag;
		c_entity_id2	:= v_rec.c_entity_id2;
		c_tin_no		:= v_rec.c_tin_no;
		c_client		:= v_rec.c_client;
		c_rplf_no		:= v_rec.c_rplf_no;
		c_cv_no			:= v_rec.c_cv_no;
		c_jv_no			:= v_rec.c_jv_no;
		c_pv_date		:= v_rec.c_pv_date;
		c_wtax_amt		:= v_rec.c_wtax_amt;
		c_net_paid		:= v_rec.c_net_paid;
		c_date_paid		:= v_rec.c_date_paid;
		c_with_lts 		:= v_rec.c_with_lts;
		c_retper 		:= v_rec.c_retper;
		c_bircode		:= v_rec.c_bircode;
		
		c_income_payment_desc	:= v_rec.c_income_payment_desc;
		c_taxrate				:= v_rec.c_taxrate;
		c_acct_name				:= v_rec.c_acct_name;
		--c_sub_projectid			:= v_rec.c_sub_projectid;
		
		c_phase			:= v_rec.c_phase;
		c_project		:= v_rec.c_project;

		c_first_day				:= v_rec.c_first_day; 
		c_last_day				:= v_rec.c_last_day; 

		c_first_day := 
		(
			case
				when p_month = '' or p_month = 'All'
					then concat(p_period1, '-', '01', '-', date_part('year', now()))
				else concat(p_month, '-', '01', '-', date_part('year', now()))
			end
		); 
		
		c_last_day := 
		(
			case
				when p_month = '' or p_month = 'All'
					then ((concat(p_period3, '-', '01', '-', date_part('year', now())))::date + interval '1 month' - interval '1 day')::date
				else (date_trunc('month', c_first_day) + interval '1 month' - interval '1 day')::date
			end
		); 
		
		--c_phase			:= coalesce(concat('Phase',(select phase from mf_sub_project where sub_proj_id = v_rec.sub_projectid and proj_id = v_rec.proj_id),''));
		--c_project		:= (select proj_name from mf_project where proj_id = v_rec.proj_id);
		
		RETURN NEXT;

	END LOOP;

END;
$BODY$;

ALTER FUNCTION public.view_ewt_forremittance_all_v2_debug(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_ewt_forremittance_all_v2_debug(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_ewt_forremittance_all_v2_debug(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.view_ewt_forremittance_all_v2_debug(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying) TO postgres;

