-- FUNCTION: public.sp_run_or(character varying)

-- DROP FUNCTION IF EXISTS public.sp_run_or(character varying);

CREATE OR REPLACE FUNCTION public.sp_run_or(
	p_user_id character varying)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare 
v_rec record;
v_co_id varchar;
v_or_date date;
BEGIN
	/*ADDED si_date by jari cruz asof jan 13 2023*/
	FOR v_rec in (
					select coalesce(x.si_date,x.or_date)::Date as or_date,  x.co_id 
					from rf_payments x
					where coalesce(x.si_date,x.or_date) is not null 
					and (x.remarks ~* 'Late LTS/BOI' or x.remarks ~* 'Late OR Issuance for Good Check')
					and coalesce(x.si_date,x.or_date)::date >= '2019-01-01'::date 
					and trim(x.status_id) = 'A' 
					and x.remarks !~* 'JV No'
					group by coalesce(x.si_date,x.or_date), x.co_id
					order by coalesce(x.si_date,x.or_date)::Date			
				) LOOP 
				
						v_or_date := v_rec.or_date;
						v_co_id   := v_rec.co_id;
						
						call sp_create_jv_late_or(v_co_id, v_or_date::date, p_user_id);

				END LOOP;

  RETURN true;
END;
$BODY$;

ALTER FUNCTION public.sp_run_or(character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.sp_run_or(character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.sp_run_or(character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.sp_run_or(character varying) TO postgres;

