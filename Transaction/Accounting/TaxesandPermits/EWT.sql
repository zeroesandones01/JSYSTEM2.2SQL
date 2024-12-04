-- select 
-- 		(
-- 			case 
-- 				when coalesce(NULLIF(TRIM(g.jv_no), ''), '') != '' 
-- 					then g.entity_id 
-- 				else (case when trim(b.rplf_type_id) = '04' then (a.entity_id) else trim(bb.entity_id2) end) 
-- 			end
-- 		) as entity_id2,
-- 		cc.tin_no,
-- 		upper(trim(c.entity_name)) as client,
-- 		a.rplf_no,
-- 		bb.cv_no,
-- 		g.jv_no as jv_no,
-- 		to_char(bb.pv_date,'MM-dd-yyyy') as pv_date,
-- 		(case when coalesce(g.jv_no, '') <> '' then g.wtax_amt else a.wtax_amt end) as wtax_amt,

-- 		/*	RIDER CHANGE	*/
-- 		(
-- 			case
-- 				when coalesce(g.jv_no, '') <> ''
-- 					then j.net_paid
-- 				else 
-- 				(
-- 					case
-- 						when coalesce(h.wtax_rate, 0) = 0
-- 							then null 
-- 						else
-- 							--case when a.vat_amt != 0 then 
-- 								a.exp_amt --DEFAULT BY LESTER TO EXP AMOUNT BECAUSE OF WRONG COMPUTATION FOR DECIMAL PLACES
-- 							--else --REPLACE THIS WHEN WRONG COMPUTATION FOR NET AMOUNT
-- 						       --(a.wtax_amt /  (ROUND(h.wtax_rate::DECIMAL, 2) / 100))::numeric(19, 2) --replace with amount because of wrong computation of amount
-- 							--end
-- 					end
-- 				)

-- 			end
-- 		) as net_paid, 
		
-- 		to_char(e.date_paid,'MM-dd-yyyy') as date_paid, 
-- 		(case when a.sub_projectid is not null or a.sub_projectid = '' then true else (case when f.lts_date is null then false else true end) end) as with_lts,
-- 		--concat_ws('/', LPAD(DATE_PART('Month', b.rplf_date)::CHAR(2), 2, '0'), 	RIGHT(DATE_PART('YEAR', b.rplf_date)::CHAR(4), 2)) as RetPer, 
-- 		concat_ws('/', LPAD(DATE_PART('Month', bb.pv_date)::CHAR(2), 2, '0'), 	RIGHT(DATE_PART('YEAR', bb.pv_date)::CHAR(4), 2)) as RetPer, --DCRF 3138 
-- 		h.wtax_bir_code as bircode, h.income_payment_desc, ROUND(h.wtax_rate::DECIMAL, 2) as tax_rate, 
-- 		i.acct_name, 
-- 		coalesce(concat('Phase',(select phase from mf_sub_project where sub_proj_id = a.sub_projectid and proj_id = a.project_id and status_id != 'I'),'')) as phase,
-- 		(select proj_name from mf_project where proj_id = a.project_id) as proj_name
		
		SELECT *
		from 
		(
			select * 
			from rf_request_detail  x
			where x.status_id != 'I'
			and x.wtax_amt != 0
			and x.co_id = '04'
			and x.rplf_no IN ('000002273')
			--and x.rplf_no in ('000002271', '000002273')
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
		and a.co_id = '04' 

		-- Modified by Mann2x; Date Modified: February 13, 2017; The date filter for JV numbers shoud be the date from the liquidation;
		-- and trim(to_char(bb.pv_date, 'yyyy')) = '2024'
		and 
		(
			case
				when coalesce(g.jv_no, '') <> ''
					then trim(to_char(g.jv_date, 'yyyy'))
				else trim(to_char(bb.pv_date, 'yyyy'))
			end
		) = '2024'

		and (case when '' = '' then true else (case when trim(b.rplf_type_id) = '04' then (a.entity_id) else trim(b.entity_id1) end) = '' end)
		and (case when '' = '' then true else b.rplf_no = '' end)
		and (case when '' = '' then true else b.rplf_no is null end)
		
		-- Modified by Mann2x; Date Modified: February 13, 2017; The date filter for JV numbers shoud be the date from the liquidation;
		-- and (case when '2024' = '' then true else trim(to_char(bb.pv_date, 'yyyy')) = '2024' end)
		and 
		(
			case
				when coalesce(g.jv_no, '') <> ''
					then (case when '2024' = '' then true else trim(to_char(g.jv_date, 'yyyy')) = '2024' end)
				else (case when '2024' = '' then true else trim(to_char(bb.pv_date, 'yyyy')) = '2024' end)
			end
		)
		
		-- Modified by Mann2x; Date Modified: February 13, 2017; The date filter for JV numbers shoud be the date from the liquidation;
		-- and (case when 'All' = 'All' then true else substr(trim(to_char(bb.pv_date, 'MM-dd-yyyy')),0,3) in ('10','11','12') end)
		-- and (case when '10' = '' then true else substr(trim(to_char(bb.pv_date, 'MM-dd-yyyy')),0,3) = '10' end)

		and 
		(
			case
				when coalesce(g.jv_no, '') <> ''
					then (case when 'All' = 'All' then true else substr(trim(to_char(g.jv_date, 'MM-dd-yyyy')), 0, 3) in ('10', '11', '12') end)
				else (case when 'All' = 'All' then true else substr(trim(to_char(bb.pv_date, 'MM-dd-yyyy')), 0, 3) in ('10', '11', '12') end)
			end
		)
		and 
		(
			case
				when coalesce(g.jv_no, '') <> ''
					then (case when '10' = '' then true else substr(trim(to_char(g.jv_date, 'MM-dd-yyyy')), 0, 3) = '10' end)
				else (case when '10' = '' then true else substr(trim(to_char(bb.pv_date, 'MM-dd-yyyy')), 0, 3) = '10' end)
			end
		)
		and (case when '' = '' then true else a.rplf_no = '' end)
		and (case when '' = '' then true else a.acct_id like '' end)
		and (case when '' = '' then true else b.entity_type_id = '' end)
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