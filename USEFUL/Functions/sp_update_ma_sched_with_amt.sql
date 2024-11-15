-- FUNCTION: public.sp_update_ma_sched_with_amt(character varying, character varying, character varying, integer, date)

-- DROP FUNCTION IF EXISTS public.sp_update_ma_sched_with_amt(character varying, character varying, character varying, integer, date);

CREATE OR REPLACE FUNCTION public.sp_update_ma_sched_with_amt(
	p_entity_id character varying,
	p_proj_id character varying,
	p_pbl_id character varying,
	p_seq_no integer,
	p_scheddate date)
    RETURNS TABLE(c_client_name character varying, c_entity_id character varying, c_proj_id character varying, c_pbl_id character varying, c_seq_no integer) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
  v_recClients RECORD;
  v_Request_Details record;
  v_Pagibig_Computation record;
  v_MA_Sched NUMERIC;
  v_Misc_Fee_Sched NUMERIC;
  v_Schedule record;
  v_Balance numeric;
  v_Proc_Fee numeric;
  v_Scheddate TIMESTAMP WITHOUT TIME ZONE;
  v_Interest NUMERIC;
  v_MA_Amount NUMERIC;
  v_Last_Sched_ID INTEGER;
  v_DaysDiff INTEGER;
  v_Principal NUMERIC;
  --v_rowPayments card_payments%ROWTYPE;

BEGIN

  --select into v_Request_Details * from req_rt_request_header where request_no = p_request_no;
  --select into v_Pagibig_Computation * from rf_pagibig_computation where entity_id = v_Request_Details.new_entity_id and proj_id = v_Request_Details.new_proj_id and pbl_id = getinteger(v_Request_Details.new_unit_id)::varchar and seq_no = v_Request_Details.new_seq_no and status_id = 'A';
  --v_Proc_Fee  := (SELECT sum(proc_fee) from rf_client_schedule where entity_id = v_Request_Details.new_entity_id and proj_id = v_Request_Details.new_proj_id and pbl_id = getinteger(v_Request_Details.new_unit_id)::varchar and seq_no = v_Request_Details.new_seq_no and part_id = '013' and status_id = 'A');
  
  v_Balance := 323594.05;
  v_Scheddate := '2024-10-14';
  
  v_Last_Sched_ID := (SELECT client_sched_id FROM rf_client_schedule where entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no ORDER BY scheddate DESC, client_sched_id DESC LIMIT 1);

  FOR v_recClients IN (SELECT *
		       FROM rf_client_schedule
		       where entity_id = p_entity_id 
		       and proj_id = p_proj_id 
		       and pbl_id = p_pbl_id 
		       and seq_no = p_seq_no 
		       and part_id IN ('014')
		       and scheddate::dATE >= p_scheddate::DATE
		       AND status_id = 'A'
		       ORDER by scheddate::dATE, client_sched_id) LOOP

        --IF v_recClients.client_ledger_id = 114585 then
        --if v_recClients.client_sched_id = 700277 then
		
	   v_DaysDiff := EXTRACT('day' FROM timestamptz_mi(v_recClients.scheddate::DATE, v_Scheddate))::int;
	   
	   v_Scheddate := v_recClients.scheddate::dATE;
	   
	   v_Interest := ROUND(((((v_Balance*.18))*v_DaysDiff)/365), 2);
		
	   IF v_recClients.scheddate::DATE = '2024-11-14' THEN
	       v_MA_Amount := 16800.00;
		   --v_Interest  := 5746.36;
	   ELSE
	       v_MA_Amount := v_recClients.amount;
	   END IF;
	   
	   v_Principal := v_MA_Amount - v_recClients.mri - v_Interest - v_recClients.fire;
	   v_Balance := ROUND((v_Balance - v_Principal), 2);
	   
	   IF v_recClients.client_sched_id = v_Last_Sched_ID THEN
	   	   v_Principal := v_Principal + v_Balance;
		   v_MA_Amount := round((v_Principal + v_recClients.mri + v_Interest), 2);
		   v_Balance   := 0;
	   END IF;
	   
	   RAISE INFO '*************************************';
	   RAISE INFO 'Scheddate: %',  v_recClients.scheddate;
	   RAISE INFO 'Amount :%', v_MA_Amount;
	   RAISE INFO 'Principal: %', v_Principal;
	   RAISE INFO 'Interest: %', v_Interest;
	   RAISE INFO 'Balance: %', v_Balance;
	   RAISE INFO '*************************************';
	   
	   UPDATE rf_client_schedule set amount = v_MA_Amount, interest = v_Interest, principal = v_Principal, balance = v_Balance where entity_id = p_entity_id and proj_id = p_proj_id and pbl_id = p_pbl_id and seq_no = p_seq_no and scheddate::DATE = v_recClients.scheddate::dATE and client_sched_id = v_recClients.client_sched_id and status_id = 'A';
        --else

        --end if;
           
	--else

	--end if;
        
  END LOOP;
 
END;
$BODY$;

ALTER FUNCTION public.sp_update_ma_sched_with_amt(character varying, character varying, character varying, integer, date)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.sp_update_ma_sched_with_amt(character varying, character varying, character varying, integer, date) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.sp_update_ma_sched_with_amt(character varying, character varying, character varying, integer, date) TO employee;

GRANT EXECUTE ON FUNCTION public.sp_update_ma_sched_with_amt(character varying, character varying, character varying, integer, date) TO postgres;

