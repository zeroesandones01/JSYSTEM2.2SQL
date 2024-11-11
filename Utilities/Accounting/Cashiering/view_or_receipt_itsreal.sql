-- FUNCTION: public.view_or_receipt_itsreal(character varying, character varying)

-- DROP FUNCTION IF EXISTS public.view_or_receipt_itsreal(character varying, character varying);

CREATE OR REPLACE FUNCTION public.view_or_receipt_itsreal(
	p_client_seqno character varying,
	p_receipt_no character varying)
    RETURNS TABLE(c_trans_date timestamp without time zone, c_or_no character varying, c_received_from character varying, c_tin_no character varying, c_address text, c_total_amount numeric, c_total_amount_words text, c_amount numeric, c_amount_words text, c_particulars character varying, c_cop character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
	v_strEntityID VARCHAR;
	v_strEntityName VARCHAR;
	v_strProjectAlias VARCHAR;
	v_strUnitDescription VARCHAR;
	v_booCash BOOLEAN;
	v_booCheck BOOLEAN;
	v_booBoth BOOLEAN;

	v_numVatSales NUMERIC;
	v_num12PercentVatSales NUMERIC;
	v_numVatExemptSales NUMERIC;

	v_recDetails RECORD;
	v_strUnitID VARCHAR;
	v_recPayments RECORD; 
	v_LateORGoodCheck BOOLEAN;
	v_RecLateORGoodCheck RECORD; 
	
	v_CreditOfPayment BOOLEAN; 
 	v_request_no VARCHAR; 
	v_DirectDeposit BOOLEAN; 
	v_co_ID VARCHAR;
	v_Payment RECORD;

BEGIN

v_co_ID := (Select distinct on (client_seqno) co_id from rf_payments where client_seqno = p_client_seqno);

v_LateORGoodCheck	:= EXISTS (SELECT * FROM rf_payments where client_seqno = p_client_seqno and or_no = p_receipt_no and TRIM(status_id) = 'A' and remarks ~* 'LATE OR Issuance for Good Check' 
								and proj_server is not null and server_id is not null);
								
SELECT INTO v_Payment * FROM rf_payments where TRIM(client_seqno) = TRIM(p_client_seqno) and TRIM(or_no) = TRIM(p_receipt_no);

											
-- ADDED BY MONIQUE DTD 2023-02-22 BASED ON DCRF #2470
v_DirectDeposit		:= (SELECT EXISTS(SELECT * FROM rf_payments a
												WHERE a.client_seqno = p_client_seqno
												AND a.or_no = p_receipt_no 				   
												AND a.remarks ~* 'Direct Deposit'
												AND a.status_id = 'A'));
							

IF p_client_seqno in ('I010211221011', 'I010220124009', 'I010220120006') THEN  -- FOR ITSREAL CLIENTS - PAYMENTS W/ SAME SEQ. NO 
	
	FOR v_recDetails IN 
	(
		SELECT *
		FROM rf_payments
		where client_seqno = p_client_seqno
	) LOOP

		

	END LOOP;
	
END IF;	


END;
$BODY$;

ALTER FUNCTION public.view_or_receipt_itsreal(character varying, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_or_receipt_itsreal(character varying, character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_or_receipt_itsreal(character varying, character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.view_or_receipt_itsreal(character varying, character varying) TO postgres;

COMMENT ON FUNCTION public.view_or_receipt_itsreal(character varying, character varying) IS 'Function to display the OR Receipt Details for Special Cases ItsReal Clients';