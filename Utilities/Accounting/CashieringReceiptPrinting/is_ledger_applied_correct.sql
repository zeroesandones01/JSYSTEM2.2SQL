-- FUNCTION: public.is_ledger_applied_correct(character varying)

-- DROP FUNCTION IF EXISTS public.is_ledger_applied_correct(character varying);

CREATE OR REPLACE FUNCTION public.is_ledger_applied_correct(
	p_client_seqno character varying)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
  v_recPayments RECORD;

  v_LedgerTotal numeric;
  v_Ledger_Correct boolean := true;
  --v_rowPayments card_payments%ROWTYPE;

BEGIN
FOR v_recPayments IN (SELECT *
		      FROM rf_payments a
		      left JOIN mf_pay_particular b on b.pay_part_id = a.pay_part_id
		      WHERE a.client_seqno = p_client_seqno
		      and a.status_id = 'A'
		      AND b.apply_ledger
		      AND EXISTS (SELECT *
				  from rf_client_ledger
				  where entity_id = a.entity_id
				  and proj_id = a.proj_id
				  and pbl_id = a.pbl_id
				  and seq_no = a.seq_no
				  and pay_rec_id = a.pay_rec_id 
				  and status_id = 'A')
			  and not exists (SELECT *
							  FROM rf_itsreal_bir_soa
							  where trim(entity_id) = trim(a.entity_id)
							  and trim(proj_id) = trim(a.proj_id)
							  and trim(pbl_id) = trim(a.pbl_id)
							  and seq_no = a.seq_no
							  and status_id = 'A'
							  )
		      order by b.apply_order) LOOP

       
	v_LedgerTotal := (SELECT sum(COALESCE(amount, 0.00)) from rf_client_ledger where entity_id = v_recPayments.entity_id and proj_id = v_recPayments.proj_id and pbl_id = v_recPayments.pbl_id and seq_no = v_recPayments.seq_no AND pay_rec_id = v_recPayments.pay_rec_id and status_id = 'A');

	if v_recPayments.amount != COALESCE(v_LedgerTotal, 0.00) then

		v_Ledger_Correct := false;
		exit;
		 --RETURN v_Ledger_Correct;
	end if;
       

  END LOOP;

  RETURN v_Ledger_Correct;

END;
$BODY$;

ALTER FUNCTION public.is_ledger_applied_correct(character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.is_ledger_applied_correct(character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.is_ledger_applied_correct(character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.is_ledger_applied_correct(character varying) TO postgres;

comment on function public.is_ledger_applied_correct(character varying) is 'Function used to check if total ledger applied is equal to total payments'