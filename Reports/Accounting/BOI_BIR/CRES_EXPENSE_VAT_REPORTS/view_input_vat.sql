-- FUNCTION: public.view_input_vat(character varying, character varying, character varying, character varying)

-- DROP FUNCTION IF EXISTS public.view_input_vat(character varying, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION public.view_input_vat(
	p_co_id character varying,
	p_project character varying,
	p_date_from character varying,
	p_date_to character varying)
    RETURNS TABLE(c_tin character varying, c_payee character varying, c_availer character varying, c_refdoc character varying, c_refdate timestamp without time zone, c_payeeadd character varying, c_availeradd character varying, c_tranamt numeric, c_netamt numeric, c_vat numeric, c_goods_services character varying, c_account_name character varying, c_wtax_amt numeric, c_input_vat_sub_group character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE 

v_vat RECORD;
v_Input_VAT_Acct_ID VARCHAR;

BEGIN
	/*EDITED BY JED 2021-03-17 : CHANGE THE FUNCTION IN GETTING THE ADDRESS OF AVAILER AND PAYEE*/
	
	delete from mf_current_address; 
	insert into mf_current_address
	SELECT _a.entity_id, max(_a.date_created), (trim(_a.addr_no) || ' ' || UPPER(trim(_a.street)) || COALESCE(', ' || UPPER(trim(_a.barangay)), '') || 
	coalesce(UPPER(trim(coalesce(', ' || _b.city_name || ' CITY', ''))), '') || UPPER(trim(coalesce(', ' || trim(_d.province_name), '')))) as address
	from rf_entity_address _a
	left join mf_city _b on _b.city_id = _a.city_id
	left join mf_address_type _c on _c.addr_type_id = _a.addr_type
	left join mf_province _d on _d.province_id = _a.province_id
	left join mf_country _e on _e.country_id = _a.country_id
	left join mf_home_ownership_type _f on _f.ownership_id = _a.ownership_id
	left join mf_municipality _g on _g.municipality_id = _a.municipality_id
	LEFT JOIN mf_zip_codes _h on _h.zip_code = _a.zip_code and _g.municipality_id = _h.municipality_id
	left join mf_region _i on _d.region_id = _i.region_id
	where (case when exists(select * from rf_entity_address y where _a.entity_id = y.entity_id and y.status_id = 'A' and y.pref_billing) then _a.pref_billing else _a.pref_cts_address end)
	and _a.status_id = 'A'
	group by _a.entity_id, _a.addr_no, _a.street, _a.barangay, _b.city_name, _d.province_name; 

	FOR v_vat IN
	(

		SELECT A."TIN", A."Payee", A."Availer", 
		(CASE WHEN COALESCE(A.JV, '') = '' THEN 'PV ' || A.PV ELSE 'JV ' || A.JV END) AS "Ref Doc. No.",
		(CASE WHEN COALESCE(A.JV, '') = '' THEN A.PV_Date ELSE A.JV_Date END)::DATE AS "Doc. Date",
		A."Payee Address", A."Availer Address", 
-- 		((CASE WHEN ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) <= 0 THEN A.Tran_Amt ELSE ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) END) + A.VAT_Amt::DECIMAL) AS "Trans Amt", 
-- 		(CASE WHEN ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) <= 0 THEN A.Tran_Amt ELSE ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) END) AS "Net Amount", 
		A.Tran_Amt AS "Trans Amt", 
		A.exp_amt AS "Net Amount",
		A.VAT_Amt AS VAT, A."Good/Services", A.co_id, A.acct_name as "Account Name", A."wTax Amount", A.entity_id1, A.entity_id2, A.tbl, A.acct_id
		FROM
		(
			SELECT F.tin_no as "TIN", 
			TRIM(D.entity_name) as "Payee", 
			TRIM(E.entity_name) as "Availer", 
			B.rplf_no AS PV, 
			(SELECT X.pv_date FROM rf_pv_header X WHERE X.pv_no = B.rplf_no and x.co_id = a.co_id) AS PV_Date, 
			''::varchar(55) AS JV, 
			null::Date AS JV_Date, 
			A.co_id, 

			--h.address as "Payee Address",
			--i.address as "Availer Address", 
			(
				case
						when A.entity_id = '9065461996' then (select company_address from mf_company where co_id = '02')
						else UPPER(get_client_address_for2307(a.entity_id)) end
			) as "Payee Address",
			(
				case
						when C.entity_id2 = '9065461996' then (select company_address from mf_company where co_id = '02')
						else UPPER(get_client_address_for2307(a.entity_id)) end
			)as "Availer Address", 

			A.amount AS Tran_Amt, 
			A.vat_amt AS Vat_Amt, 
			A.entity_id, A.entity_type_id AS Type, 
			(CASE WHEN A.entity_type_id IN ('05', '06', '19', '20', '25', '09', '10') THEN 'VAT ON PURCHASE OF GOODS' ELSE 'VAT ON PURCHASE OF SERVICES' END) AS "Good/Services", 
			g.acct_name, 
			a.wtax_amt as "wTax Amount", 
			B.rplf_type_id, 
			a.entity_id as entity_id1, 
			c.entity_id2 as entity_id2, 
			A.exp_amt, 'pv' as tbl,null as acct_id -- j.acct_id
			FROM (SELECT * FROM rf_request_detail X WHERE X.status_id != 'I' and (x.co_id = p_co_id OR p_co_id = '')) A
			LEFT JOIN (SELECT * from rf_request_header X WHERE X.status_id != 'I') B ON A.rplf_no = B.rplf_no AND A.co_id = B.co_id
			LEFT JOIN (SELECT * from rf_pv_header X WHERE X.status_id = 'P') C ON A.rplf_no = C.rplf_no AND A.co_id = C.co_id
			--LEFT JOIN rf_entity D ON A.entity_id = D.entity_id --replace by code below DCRF 3155
			LEFT JOIN rf_entity D ON c.entity_id2 = D.entity_id 
			LEFT JOIN rf_entity E ON C.entity_id2 = E.entity_id
			LEFT JOIN rf_entity_id_no F ON A.entity_id = F.entity_id
			left join mf_boi_chart_of_accounts g on g.acct_id = a.acct_id
			left join mf_current_address h on h.entity_id = a.entity_id
			left join mf_current_address i on i.entity_id = a.entity_id
			--LEFT JOIN rf_pv_detail j on j.pv_no = A.rplf_no and j.co_id = p_co_id and j.acct_id IN ('01-99-07-000', '01-99-03-000', '01-99-06-000') AND j.status_id = 'A' AND j.tran_amt = a.vat_amt
			WHERE
			A.entity_type_id IN ('02', '12', '05', '06', '19', '20', '25', '09', '10', '11', '07', '08', '15', 
			'16', '17', '18', '23', '24', '34', '35', '32', '33', '03', '04', '14', '38', '39', '40', '41', '42', '43', '36')
			and B.rplf_type_id NOT in ('02', '07') 
			and not exists (select pv_no from rf_pv_header where pv_no = B.rplf_no and status_id in ('I', 'F', 'D') and co_id = p_co_id)
		) A
		WHERE (CASE WHEN COALESCE(A.JV, '') = '' THEN A.PV_Date ELSE A.JV_Date END)::DATE BETWEEN p_date_from::date and p_date_to::date
		--AND A.VAT_Amt != 0

		UNION all

-- 		SELECT A."TIN", A."Payee", A."Availer", 
-- 		(CASE WHEN COALESCE(A.JV, '') = '' THEN 'PV ' || A.PV ELSE 'JV ' || A.JV END) AS "Ref Doc. No.",
-- 		(CASE WHEN COALESCE(A.JV, '') = '' THEN A.PV_Date ELSE A.JV_Date END)::DATE AS "Doc. Date",
-- 		A."Payee Address", A."Availer Address", 
-- 		((CASE WHEN ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) <= 0 THEN A.Tran_Amt ELSE ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) END) + A.VAT_Amt::DECIMAL) AS "Trans Amt", 
-- 		(CASE WHEN ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) <= 0 THEN A.Tran_Amt ELSE ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) END) AS "Net Amount", 
-- 		A.VAT_Amt AS VAT, A."Good/Services", A.co_id, A.acct_name as "Account Name", A."wTax Amount", A.entity_id1, A.entity_id2
-- 		FROM
-- 		(
-- 			select
-- 			coalesce(replace(REPLACE(coalesce(e.tin_no, f.tin_no), '-', ''), ' ', ''), replace(REPLACE(coalesce(k.tin_no, l.tin_no), '-', ''), ' ', '')) as "TIN", 
-- 			coalescE(j.entity_name, d.entity_name) as "Payee", 
-- 			h.entity_name as "Availer", 
-- 			''::varchar(55) as PV, null::Date as PV_Date, 
-- 			a.jv_no as JV, a.jv_date::date as JV_Date, A.co_id, 

-- 			m.address as "Payee Address", n.address as "Availer Address", 

-- 			(
-- 				case
-- 					when c.entity_id is not null
-- 						then 
-- 						(
-- 							case 
-- 								when coalesce(c.vat_amt, 0)::numeric(19, 2) = 0::numeric(19, 2) or c.vat_amt::numeric(19, 2) is null or c.vat_amt::numeric(19, 2) = 0::numeric(19, 2)
-- 									then c.tran_amt
-- 								else ROUND((c.vat_amt::DECIMAL / 0.12::DECIMAL), 2) + c.vat_amt 
-- 							end
-- 						) 
-- 					else 
-- 						(
-- 							case 
-- 								when coalesce(i.vat_amt, 0)::numeric(19, 2) = 0::numeric(19, 2) or i.vat_amt::numeric(19, 2) is null or i.vat_amt::numeric(19, 2) = 0::numeric(19, 2)
-- 									then i.trans_amt
-- 								else ROUND((i.vat_amt::DECIMAL / 0.12::DECIMAL), 2) + i.vat_amt
-- 							end
-- 						) 
-- 				end
-- 			) as Tran_Amt, 
-- 			(
-- 				case
-- 					when c.entity_id is not null
-- 						then 
-- 						(
-- 							case 
-- 								when coalesce(c.vat_amt, 0)::numeric(19, 2) = 0::numeric(19, 2) or c.vat_amt::numeric(19, 2) is null or c.vat_amt::numeric(19, 2) = 0::numeric(19, 2)
-- 									then c.tran_amt
-- 								else ROUND((c.vat_amt::DECIMAL / 0.12::DECIMAL), 2)
-- 							end
-- 						) 
-- 					else
-- 						(
-- 							case 
-- 								when coalesce(i.vat_amt, 0)::numeric(19, 2) = 0::numeric(19, 2) or i.vat_amt::numeric(19, 2) is null or i.vat_amt::numeric(19, 2) = 0::numeric(19, 2)
-- 									then i.trans_amt
-- 								else ROUND((i.vat_amt::DECIMAL / 0.12::DECIMAL), 2)
-- 							end
-- 						) 
-- 				end		
-- 			) as Net_Amt, 
-- 			(
-- 				case
-- 					when c.vat_amt is null
-- 						then o.tran_amt
-- 					else coalesce(c.vat_amt, 0)::numeric(19, 2) 
-- 				end
-- 			) as Vat_Amt, 
			
-- 			coalesce(i.entity_id, c.entity_id) as entity_id, coalesce(i.entity_type_id, c.entity_type_id) as "type", 
-- 			(CASE WHEN c.entity_type_id IN ('05', '06', '19', '20', '25', '09', '10') THEN 'VAT ON PURCHASE OF GOODS' ELSE 'VAT ON PURCHASE OF SERVICES' END) AS "Good/Services", 
-- 			p.acct_name as acct_name, coalesce(coalesce(c.wtax_amt, c.wtax_amt), 0)::numeric(19, 2) as "wTax Amount", g.rplf_type_id, coalesce(i.entity_id, c.entity_id) as entity_id1, g.entity_id2 as entity_id2
-- 			from 
-- 			(
-- 				select * 
-- 				from rf_jv_header x
-- 				where x.status_id = 'P'
-- 				--and not exists(select * from rf_subsidiary_ledger y where y.jv_no = x.jv_no)
-- 			) a 
-- 			left join (select * from rf_liq_header where status_id != 'I') b on b.jv_no = a.jv_no 
-- 			left join (select * from rf_liq_detail where status_id != 'I') c on b.liq_no = c.liq_no 
-- 			--Modified by Mann2x; Date Modified: December 18, 2018; This particular JV# 18110001 has no liquidation entry thus returns no row when 
-- 			--JOINED with rf_entity;	This particular payment request is categorized as direct expense hence the absence of liquidation entry;
-- 			--inner join rf_entity d on c.entity_id = d.entity_id
-- 			left join rf_entity d on c.entity_id = d.entity_id
-- 			LEFT JOIN rf_entity_id_no e on d.entity_id = e.entity_id
-- 			left join (select x.entity_id, y.tin_no from em_employee x left join rf_entity_id_no y on x.entity_id = y.entity_id) f on c.entity_id = f.entity_id
-- 			left join (SELECT * from rf_request_header X WHERE X.status_id != 'I') g ON b.rplf_no = g.rplf_no AND a.co_id = g.co_id
-- 			left join rf_entity h on h.entity_id = g.entity_id2
-- -- 			left join 
-- -- 			(
-- -- 				select sum(trans_amt) as trans_amt, sum(vat_amt) as vat_amt, co_id, jv_no, entity_id, entity_type_id, sundry_acct
-- -- 				from rf_subsidiary_ledger 
-- -- 				where status_id != 'I'
-- -- 				group by co_id, jv_no, entity_id, entity_type_id, sundry_acct
-- -- 			) i on i.jv_no = a.jv_no and a.co_id = i.co_id and i.entity_id = c.entity_id and i.sundry_acct = c.acct_id
-- 			left join 
-- 			(
-- 				select sum(trans_amt) as trans_amt, sum(vat_amt) as vat_amt, co_id, jv_no, entity_id, entity_type_id, sundry_acct, liq_row
-- 				from rf_subsidiary_ledger 
-- 				where status_id != 'I'
-- 				group by co_id, jv_no, entity_id, entity_type_id, sundry_acct, liq_row
-- 			) i on i.jv_no = a.jv_no and a.co_id = i.co_id and i.entity_id = c.entity_id and i.liq_row = c.liq_row
-- 			left join rf_entity j on j.entity_id = i.entity_id
-- 			LEFT JOIN rf_entity_id_no k on j.entity_id = k.entity_id
-- 			left join (select x.entity_id, y.tin_no from em_employee x left join rf_entity_id_no y on x.entity_id = y.entity_id) l on l.entity_id = k.entity_id
-- 			left join mf_current_address m on m.entity_id = coalesce(i.entity_id, c.entity_id)
-- 			left join mf_current_address n on n.entity_id = g.entity_id2
-- 			left join 
-- 			(
-- 				select sum(x.tran_amt) as tran_amt, x.jv_no, x.co_id, x.acct_id, x.status_id
-- 				from rf_jv_detail x 
-- 				where x.acct_id = '01-99-03-000' and x.status_id = 'A'
-- 				group by x.jv_no, x.co_id, x.acct_id, x.status_id
-- 			) o on o.jv_no = a.jv_no and o.co_id = a.co_id 
-- 			left join mf_boi_chart_of_accounts p on p.acct_id = coalesce(i.sundry_acct, c.acct_id)
-- 		) A
-- 		WHERE (A.co_id = p_co_id OR p_co_id = '') AND (CASE WHEN COALESCE(A.JV, '') = '' THEN A.PV_Date ELSE A.JV_Date END)::DATE BETWEEN p_date_from::date and p_date_to::date

-- 		union 

-- 		SELECT A."TIN", A."Payee", A."Availer", 
-- 		(CASE WHEN COALESCE(A.JV, '') = '' THEN 'PV ' || A.PV ELSE 'JV ' || A.JV END) AS "Ref Doc. No.",
-- 		(CASE WHEN COALESCE(A.JV, '') = '' THEN A.PV_Date ELSE A.JV_Date END)::DATE AS "Doc. Date",
-- 		A."Payee Address", A."Availer Address", 
-- 		((CASE WHEN ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) <= 0 THEN A.Tran_Amt ELSE ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) END) + A.VAT_Amt::DECIMAL) AS "Trans Amt", 
-- 		(CASE WHEN ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) <= 0 THEN A.Tran_Amt ELSE ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) END) AS "Net Amount", 
-- 		A.VAT_Amt AS VAT, A."Good/Services", A.co_id, A.acct_name as "Account Name", A."wTax Amount", A.entity_id1, A.entity_id2
-- 		FROM
-- 		(
-- 			select replace(REPLACE(coalesce(d.tin_no), '-', ''), ' ', '') as "TIN", c.entity_name as "Payee", g.entity_name as "Availer", 
-- 			''::varchar(55) as PV, null::Date as pv_date, a.jv_no as jv, b.jv_date::date as jv_date, b.co_id, 
-- 			h.address as "Payee Address", i.address as "Availer Address", 

-- 			coalesce(a.trans_amt, 0) as Tran_Amt, 
-- 			coalesce(a.trans_amt, 0)-wtax_amt as Net_Amt, 
-- 			coalesce(a.vat_amt, 0) as Vat_Amt, 

-- 			a.entity_id, a.entity_type_id, 
-- 			(CASE WHEN a.entity_type_id IN ('05', '06', '19', '20', '25', '09', '10') THEN 'VAT ON PURCHASE OF GOODS' ELSE 'VAT ON PURCHASE OF SERVICES' END) AS "Good/Services", 
-- 			(select x.acct_name from mf_boi_chart_of_accounts x where x.acct_id = a.sundry_acct) as acct_name, 
-- 			coalesce(a.wtax_amt, 0) as "wTax Amount", f.rplf_type_id, a.entity_id as entity_id1, f.entity_id2
-- 			from (select * from rf_subsidiary_ledger x where x.status_id = 'A') a
-- 			inner join (select * from rf_jv_header x where x.status_id = 'P') b on a.jv_no = b.jv_no and a.co_id = b.co_id
-- 			inner join rf_entity c on a.entity_id = c.entity_id
-- 			inner join (select * from rf_entity_id_no x where x.status_id = 'A') d on c.entity_id = d.entity_id
-- 			inner join (select * from rf_liq_header x where x.status_id != 'I') e on b.jv_no = e.jv_no
-- 			left join (SELECT * from rf_request_header x WHERE x.status_id != 'I') f on e.rplf_no = f.rplf_no
-- 			left join rf_entity g on g.entity_id = f.entity_id2
-- 			left join mf_current_address h on h.entity_id = c.entity_id
-- 			left join mf_current_address i on i.entity_id = f.entity_id2
-- 		) A
-- 		WHERE (A.co_id = p_co_id OR p_co_id = '') 
-- 		AND (CASE WHEN COALESCE(A.JV, '') = '' THEN A.PV_Date ELSE A.JV_Date END)::DATE BETWEEN p_date_from::date and p_date_to::date AND A.VAT_Amt != 0
		
		SELECT A."TIN", 
		A."Payee", 
		A."Availer", 
		(CASE WHEN COALESCE(A.JV, '') = '' THEN 'PV ' || A.PV ELSE 'JV ' || A.JV END) AS "Ref Doc. No.",
		(CASE WHEN COALESCE(A.JV, '') = '' THEN A.PV_Date ELSE A.JV_Date END)::DATE AS "Doc. Date",
		A."Payee Address", 
		A."Availer Address", 
		--((CASE WHEN ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) <= 0 THEN A.Tran_Amt ELSE ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) END) + A.VAT_Amt::DECIMAL) AS "Trans Amt",
		A.Tran_Amt as "Trans Amt", --replaced by lester 
		(CASE WHEN ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) <= 0 THEN A.Tran_Amt ELSE ROUND((A.VAT_Amt::DECIMAL / 0.12::DECIMAL), 2) END) AS "Net Amount", 
		A.VAT_Amt AS VAT, A."Good/Services", A.co_id, A.acct_name as "Account Name", A."wTax Amount", A.entity_id1, A.entity_id2, A.tbl, A.acct_id
		FROM
		(
			select replace(REPLACE(coalesce(d.tin_no), '-', ''), ' ', '') as "TIN", c.entity_name as "Payee", 
			g.entity_name as "Availer", 
			''::varchar(55) as PV, null::Date as pv_date, a.jv_no as jv, b.jv_date::date as jv_date, b.co_id, 
			
			--h.address as "Payee Address", 
			--i.address as "Availer Address", 
			
			(
				case
						when a.entity_id = '9065461996' then (select company_address from mf_company where co_id = '02')
						else UPPER(get_client_address_for2307(c.entity_id)) end
			) as "Payee Address",
			(
				case
						when f.entity_id2 = '9065461996' then (select company_address from mf_company where co_id = '02')
						else UPPER(get_client_address_for2307(f.entity_id2)) end
			)as "Availer Address", 
			
			coalesce(a.trans_amt, 0) as Tran_Amt, 
			coalesce(a.trans_amt, 0)-wtax_amt as Net_Amt, 
			coalesce(a.vat_amt, 0) as Vat_Amt, 
			a.entity_id, 
			a.entity_type_id, 
			(CASE WHEN a.entity_type_id IN ('05', '06', '19', '20', '25', '09', '10') THEN 'VAT ON PURCHASE OF GOODS' ELSE 'VAT ON PURCHASE OF SERVICES' END) AS "Good/Services", 
			(select x.acct_name from mf_boi_chart_of_accounts x where x.acct_id = a.sundry_acct) as acct_name, 
			coalesce(a.wtax_amt, 0) as "wTax Amount", 
			f.rplf_type_id, 
			a.entity_id as entity_id1,
			f.entity_id2, 'jv' as tbl, null as acct_id -- j.acct_id
			from (select * from rf_subsidiary_ledger x where x.status_id = 'A' AND x.co_id = p_co_id OR p_co_id = '') a
			inner join (select * from rf_jv_header x where x.status_id = 'P' AND x.co_id = p_co_id OR p_co_id = '') b on a.jv_no = b.jv_no and a.co_id = b.co_id
			inner join rf_entity c on a.entity_id = c.entity_id
			inner join (select * from rf_entity_id_no x where x.status_id = 'A') d on c.entity_id = d.entity_id
			left join (select * from rf_liq_header x where x.status_id != 'I' AND x.co_id = p_co_id OR p_co_id = '') e on b.jv_no = e.jv_no AND a.co_id = e.co_id
			left join (SELECT * from rf_request_header x WHERE x.status_id != 'I' AND x.co_id = p_co_id OR p_co_id = '') f on e.rplf_no = f.rplf_no and f.co_id = a.co_id
			left join rf_entity g on g.entity_id = f.entity_id2
			left join mf_current_address h on h.entity_id = c.entity_id
			left join mf_current_address i on i.entity_id = f.entity_id2
			--left join rf_jv_detail j on j.jv_no = a.jv_no and j.co_id = p_co_id and j.acct_id IN ('01-99-07-000', '01-99-03-000', '01-99-06-000') AND j.status_id = 'A' AND j.tran_amt = a.vat_amt
		) A
		WHERE (A.co_id = p_co_id OR p_co_id = '') 
		AND (CASE WHEN COALESCE(A.JV, '') = '' THEN A.PV_Date ELSE A.JV_Date END)::DATE BETWEEN p_date_from::date and p_date_to::date
		--AND A.VAT_Amt != 0
	                                                                                                                                                                                                                       )
	LOOP
	
		

		c_tin				:=	v_vat."TIN";
		c_payee				:=	v_vat."Payee";
		c_availer			:=	v_vat."Availer";
		c_refDoc			:=	v_vat."Ref Doc. No.";
		c_refDate			:=	v_vat."Doc. Date";
		c_payeeAdd			:=	v_vat."Payee Address";
		c_availerAdd		:=	v_vat."Availer Address";
		c_tranAmt			:=	v_vat."Trans Amt";
		c_netAmt			:=	v_vat."Net Amount";
		c_vat				:=	v_vat."vat";
		c_goods_services	:=	v_vat."Good/Services";
		c_account_name		:=	v_vat."Account Name";
		c_wtax_amt			:=	v_vat."wTax Amount";
		
		
		IF v_vat.tbl = 'pv' THEN
		
			RAISE INFO 'PVVVVVVV';
			RAISE INFO 'Rec doc no: %', v_vat."Ref Doc. No.";
			RAISE INFO 'Vat: %', v_vat."vat";
			
			c_input_vat_sub_group := (SELECT acct_name 
									  from mf_boi_chart_of_accounts 
									  where acct_id = (select acct_id 
													   from rf_pv_detail 
													   where pv_no = TRIM(REPLACE(c_refDoc, 'PV', '')) 
													   and co_id = p_co_id 
													   and acct_id iN ('01-99-07-000', '01-99-03-000', '01-99-06-000')
													   --AND tran_amt = v_vat."vat"
													   and status_id = 'A' GROUP BY acct_id)
									  AND status_id = 'A');
		else
		
			RAISE INFO 'JVVVVVVV';
			RAISE INFO 'Rec doc no: %', c_refDoc;
			RAISE INFO 'Vat: %', c_vat;
			c_input_vat_sub_group := (SELECT acct_name 
									  from mf_boi_chart_of_accounts 
									  where acct_id = (select acct_id 
													   from rf_jv_detail 
													   where jv_no = TRIM(REPLACE(c_refDoc, 'JV', '')) 
													   and co_id = p_co_id 
													   and acct_id iN ('01-99-07-000', '01-99-03-000', '01-99-06-000')
													   --AND tran_amt = c_vat
													   and status_id = 'A' GROUP BY acct_id)
									  AND status_id = 'A');
		END IF;
			
		

		RETURN NEXT;

	END LOOP;

END;
$BODY$;

ALTER FUNCTION public.view_input_vat(character varying, character varying, character varying, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_input_vat(character varying, character varying, character varying, character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_input_vat(character varying, character varying, character varying, character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.view_input_vat(character varying, character varying, character varying, character varying) TO postgres;

COMMENT ON FUNCTION public.view_input_vat(character varying, character varying, character varying, character varying) IS 'Function used to preview Input VAT Sched V2 in CRES/Expense/VAT Reports Module'