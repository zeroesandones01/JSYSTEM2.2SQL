-- View: public.view_employee

-- DROP VIEW public.view_employee;

CREATE OR REPLACE VIEW public.view_employee
 AS
 SELECT btrim(a.entity_id::text) AS "ID",
    btrim(a.entity_name::text) AS "Name",
    btrim(c.dept_alias::text) AS "Dept.",
    btrim(d.division_alias::text) AS "Div.",
    btrim(e.company_alias::text) AS "Company"
   FROM rf_entity a
     LEFT JOIN em_employee b ON b.entity_id::text = a.entity_id::text
     LEFT JOIN mf_department c ON c.dept_code::text = b.dept_code::text
     LEFT JOIN mf_division d ON d.division_code::text = b.div_code::text
     LEFT JOIN mf_company e ON e.co_id::text = b.co_id::text
  WHERE (a.entity_id::text IN ( SELECT em_employee.entity_id
           FROM em_employee)) OR a.entity_id::text = '1072088811'::text OR a.entity_id::text = '3231323152'::text OR a.entity_id::text = '9593722861'::text OR a.entity_id::text = '6044568843'::text OR a.entity_id::text = '2907888423'::text OR a.entity_id::text = '0504063223'::text OR a.entity_id::text = '7576669398'::text OR a.entity_id::text = '4136932939'::text OR a.entity_id::text = '7739837746'::text OR a.entity_id::text = '5257444432'::text OR a.entity_id::text = '9887048266'::text OR a.entity_id::text = '2182360358'::text OR a.entity_id::text = '9002395454'::text OR a.entity_id::text = '5929642685'::text OR a.entity_id::text = '5991823086'::text OR a.entity_id::text = '9553221100'::text OR a.entity_id::text = '8785068676'::text OR a.entity_id::text = '5524735599'::text OR a.entity_id::text = '6178942588'::text OR a.entity_id::text = '4884483204'::text OR a.entity_id::text = '2163208770'::text OR a.entity_id::text = '7231513880'::text 
		   OR a.entity_id::text = '8285650158'::text
  ORDER BY a.entity_name;

ALTER TABLE public.view_employee
    OWNER TO postgres;

GRANT ALL ON TABLE public.view_employee TO PUBLIC;
GRANT ALL ON TABLE public.view_employee TO postgres;

