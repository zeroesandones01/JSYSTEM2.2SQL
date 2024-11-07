-- FUNCTION: public.view_card_tcost_computation_v2_debug(character varying, character varying, character varying, integer, boolean)

-- DROP FUNCTION IF EXISTS public.view_card_tcost_computation_v2_debug(character varying, character varying, character varying, integer, boolean);

CREATE OR REPLACE FUNCTION public.view_card_tcost_computation_v2_debug(
	p_entity_id character varying,
	p_proj_id character varying,
	p_pbl_id character varying,
	p_seq_no integer,
	p_for_ecar boolean)
    RETURNS TABLE(c_select boolean, c_tcost_detail_desc character varying, c_tcost_detail_amt numeric, c_applied_amt numeric, c_remarks character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

  v_TCost_Computation record;
  v_Client_Price_History record;
  v_Total_Tcost_Amt NUMERIC;

BEGIN

	   SELECT INTO v_Client_Price_History * FROM rf_client_price_history 
                                             WHERE entity_id   = p_entity_id 
                                             AND   proj_id     = p_proj_id 
                                             AND   pbl_id      = p_pbl_id 
                                             AND   seq_no      = p_seq_no 
                                             AND   status_id   = 'A';

    IF p_for_ecar THEN
	    IF p_proj_id = '015' THEN
		
		
		ELSE
		
		END IF;
	
		FOR v_TCost_Computation IN (SELECT * FROM mf_transfer_cost_dl
									WHERE status_id   = 'A' 
									AND   proj_id       = p_proj_id
									AND  tcostdtl_id IN ('262', '095', '200', '199', '168', '178', '34', '272', '103', '266', '177', '166')
									ORDER BY tcostdtl_desc
									   ) LOOP
			c_SELECT 				:= false; 

			/**EDITED BY JED 2019-09-17 DCRF NO. 1216 : CHANGE NAME OF PARTICULAR**/
			c_tcost_detail_desc 	:= (CASE WHEN v_TCost_Computation.tcostdtl_id = '229' THEN 'SERVICE FEE - BIR' WHEN v_TCost_Computation.tcostdtl_id = '215' THEN 'CERTIFIED TRUE COPY OF TAX DECLARATION HOUSE'  -- Added by Erick 2023-07-17 DCRF 2686
											 WHEN v_TCost_Computation.tcostdtl_id = '168' THEN 'SERVICE FEE FOR CERTIFIED TRUE COPY OF TCT' 
											 WHEN v_TCost_Computation.tcostdtl_id = '180' THEN 'ANNOTATION OF SPA (IF APPLICABLE)'
											 WHEN v_TCost_Computation.tcostdtl_id = '219' THEN 'SERVICE FEE - SPA (IF APPLICABLE)'ELSE  
									   (CASE WHEN v_TCost_Computation.tcostdtl_id = '038' THEN 'SERVICE FEE - RD' WHEN v_TCost_Computation.tcostdtl_id = '216' THEN 'CERTIFIED TRUE COPY OF TAX DECLARATION LOT' ELSE v_TCost_Computation.tcostdtl_desc end)end);

			IF  NULLIF(v_TCost_Computation.udf_name, '') is NULL THEN
				--Added by Erick 2023-07-20 DCRF 2690
			
				IF v_TCost_Computation.tcostdtl_id = '229' THEN 
					c_tcost_detail_amt 	:= 2250; -- Code 229 + 267 = 3770.00
				ELSIF v_TCost_Computation.tcostdtl_id in ('215', '216') then
					c_tcost_detail_amt 	:= v_TCost_Computation.tcostdtl_amt + 20;
				ELSIF v_TCost_Computation.tcostdtl_id in ('098') THEN
					c_tcost_detail_amt  := v_TCost_Computation.tcostdtl_amt + 100;
				ELSE
					c_tcost_detail_amt 	:= v_TCost_Computation.tcostdtl_amt;
				END IF;
				
			   --c_tcost_detail_amt 	:= v_TCost_Computation.tcostdtl_amt; 
			ELSE
			--Raise info 'tcostdtl_id: %',v_TCost_Computation.tcostdtl_id;
			   c_tcost_detail_amt 	:= get_tcost_dtl_amt(v_TCost_Computation.tcostdtl_id, p_entity_id, p_proj_id, p_pbl_id, p_seq_no);
			END IF;

			c_applied_amt 			:= NULL; 
			c_remarks 				:= v_TCost_Computation.remarks;
			v_Total_Tcost_Amt 		:= COALESCE(v_Total_Tcost_Amt, 0.00) + COALESCE(c_tcost_detail_amt, 0.00);	

			RETURN NEXT;						   
									   
									   
		END LOOP;
		
		FOR v_TCost_Computation IN (SELECT * FROM mf_transfer_cost_dl
									WHERE status_id   = 'A' 
									AND   proj_id       = p_proj_id
									AND  tcostdtl_id IN ('103')
									ORDER BY tcostdtl_desc
									   ) LOOP
			c_SELECT 				:= false; 
			c_tcost_detail_desc 	:= v_TCost_Computation.tcostdtl_desc;
			c_tcost_detail_amt 		:= ROUND((v_Total_Tcost_Amt *.03), 2); 
			c_applied_amt 			:= NULL; 
			c_remarks 				:= v_TCost_Computation.remarks;

			RETURN NEXT;						   
									   
									   
		END LOOP;
    
	
	ELSE
		IF p_proj_id !='015' THEN
    
			RAISE INFO 'Rizal CLIENT!';

			FOR v_TCost_Computation IN (SELECT * FROM mf_transfer_cost_dl
													 WHERE status_id   = 'A' 
													 AND   proj_id       = p_proj_id
													 AND   for_tcostcomp is true
													 AND   tcostdtl_id NOT IN ('103', '107', '017', '116', '065','094','199','200','015','014','107', '017', '116', '065','191','212','106','217','181','173','023','024', '244', '034')
													 AND   server_id is NULL
												 ORDER BY tcostdtl_desc
									   ) LOOP
			Raise info 'tcostdtl_id: %',v_TCost_Computation.tcostdtl_id;						   

			c_SELECT 				:= false; 

			/**EDITED BY JED 2019-09-17 DCRF NO. 1216 : CHANGE NAME OF PARTICULAR**/
			c_tcost_detail_desc 	:= --(CASE WHEN v_TCost_Computation.tcostdtl_id = '020' THEN 'SERVICE FEE - BIR' WHEN v_TCost_Computation.tcostdtl_id = '215' THEN 'CERTIFIED TRUE COPY OF TAX DECLARATION HOUSE' 
										(CASE WHEN v_TCost_Computation.tcostdtl_id = '229' THEN 'SERVICE FEE - BIR' WHEN v_TCost_Computation.tcostdtl_id = '215' THEN 'CERTIFIED TRUE COPY OF TAX DECLARATION HOUSE'  -- Added by Erick 2023-07-17 DCRF 2686
											 WHEN v_TCost_Computation.tcostdtl_id = '168' THEN 'SERVICE FEE FOR CERTIFIED TRUE COPY OF TCT' 
											 WHEN v_TCost_Computation.tcostdtl_id = '180' THEN 'ANNOTATION OF SPA (IF APPLICABLE)'
											 WHEN v_TCost_Computation.tcostdtl_id = '219' THEN 'SERVICE FEE - SPA (IF APPLICABLE)'ELSE  
									   (CASE WHEN v_TCost_Computation.tcostdtl_id = '038' THEN 'SERVICE FEE - RD' WHEN v_TCost_Computation.tcostdtl_id = '216' THEN 'CERTIFIED TRUE COPY OF TAX DECLARATION LOT' ELSE v_TCost_Computation.tcostdtl_desc end)end);

			IF  NULLIF(v_TCost_Computation.udf_name, '') is NULL THEN
				--Added by Erick 2023-07-20 DCRF 2690
				IF v_TCost_Computation.tcostdtl_id = '229' THEN 
					c_tcost_detail_amt 	:= 3770.00; -- Code 229 + 231 = 3770.00
				ELSE
					c_tcost_detail_amt 	:= v_TCost_Computation.tcostdtl_amt;
				END IF;

			   --c_tcost_detail_amt 	:= v_TCost_Computation.tcostdtl_amt; 
			ELSE
			--Raise info 'tcostdtl_id: %',v_TCost_Computation.tcostdtl_id;
			   c_tcost_detail_amt 	:= get_tcost_dtl_amt(v_TCost_Computation.tcostdtl_id, p_entity_id, p_proj_id, p_pbl_id, p_seq_no);
			END IF;

			c_applied_amt 			:= NULL; 
			c_remarks 				:= v_TCost_Computation.remarks;
			v_Total_Tcost_Amt 		:= COALESCE(v_Total_Tcost_Amt, 0.00) + COALESCE(c_tcost_detail_amt, 0.00);	

			RETURN NEXT;
			END LOOP;

			RAISE INFO 'Total TCost Amt: %', v_Total_Tcost_Amt;

			/**ADDED BY JED 2019-09-17 DCRF NO. 1216 : CHANGE NAME OF PARTICULAR**/
			FOR v_TCost_Computation IN (SELECT sum(tcostdtl_amt) as tcostdtl_amt,remarks       
										FROM mf_transfer_cost_dl
													WHERE   status_id = 'A' 
													AND     proj_id   = p_proj_id
													AND     for_tcostcomp is true
													AND     tcostdtl_id   in ('065')
													AND     server_id     is NULL
										GROUP BY remarks
									   ) LOOP

			c_SELECT 				:= false; 
			c_tcost_detail_desc 	:= 'SERVICE FEE-ASSESSORS OFFICE (LOT)';
			c_tcost_detail_amt 		:= v_TCost_Computation.tcostdtl_amt;
			c_applied_amt 			:= NULL; 
			c_remarks 				:= v_TCost_Computation.remarks;
			v_Total_Tcost_Amt 		:= COALESCE(v_Total_Tcost_Amt, 0.00) + COALESCE(c_tcost_detail_amt, 0.00);	

			RETURN NEXT;
			END LOOP;

			FOR v_TCost_Computation IN (SELECT sum(tcostdtl_amt) as tcostdtl_amt, remarks
										FROM mf_transfer_cost_dl
													WHERE   status_id   = 'A' 
													AND     proj_id     = p_proj_id
													AND     for_tcostcomp is true
													AND     tcostdtl_id   in ('116')
													AND     server_id     is NULL
										 GROUP BY remarks
									   ) LOOP

			c_SELECT 				:= false; 
			c_tcost_detail_desc 	:= 'SERVICE FEE-ASSESSORS OFFICE (HOUSE)';
			c_tcost_detail_amt 		:= v_TCost_Computation.tcostdtl_amt;
			c_applied_amt 			:= NULL; 
			c_remarks 				:= v_TCost_Computation.remarks;
			v_Total_Tcost_Amt 		:= COALESCE(v_Total_Tcost_Amt, 0.00) + COALESCE(c_tcost_detail_amt, 0.00);	

			RETURN NEXT;
			END LOOP;

			RAISE INFO 'Total TCost Amt: %', v_Total_Tcost_Amt;

				FOR v_TCost_Computation IN (SELECT * FROM mf_transfer_cost_dl
															WHERE status_id = 'A' 
															AND proj_id = p_proj_id
															AND for_tcostcomp is true
															AND tcostdtl_id = '103'
															AND server_id is NULL
											ORDER BY tcostdtl_desc
										   ) LOOP

			c_SELECT 				:= false; 
			c_tcost_detail_desc 	:= v_TCost_Computation.tcostdtl_desc;

			c_tcost_detail_amt 		:= ROUND((v_Total_Tcost_Amt *.03), 2); 

			c_applied_amt 			:= NULL; 
			c_remarks 				:= v_TCost_Computation.remarks;

			RETURN NEXT;
			END LOOP;

			ELSE 

			RAISE INFO 'CAVITE CLIENT!';

		   SELECT INTO v_Client_Price_History * FROM rf_client_price_history 
														   WHERE entity_id = p_entity_id
														   AND   proj_id   = p_proj_id
														   AND   pbl_id    = p_pbl_id
														   AND   seq_no    = p_seq_no
														   AND   status_id = 'A';

			FOR v_TCost_Computation IN (SELECT * FROM mf_transfer_cost_dl
														   WHERE status_id = 'A' 
														   AND proj_id = p_proj_id
														   AND for_tcostcomp is true
														   AND tcostdtl_id NOT IN ('103', '107', '017', '116', '065','094','014','015','107', '017', '116', '065','191','212','106','217','181','173','023','024','244','230')
														   AND server_id is NULL
										ORDER BY tcostdtl_desc
									   ) LOOP

			c_SELECT 				:= false; 

			/**EDITED BY JED 2019-09-17 DCRF NO. 1216 : CHANGE NAME OF PARTICULAR**/
			c_tcost_detail_desc 	:= (CASE WHEN v_TCost_Computation.tcostdtl_id = '020' THEN 'SERVICE FEE - BIR' WHEN v_TCost_Computation.tcostdtl_id = '199' THEN 'CERTIFIED TRUE COPY OF TAX DECLARATION LOT'
											 WHEN v_TCost_Computation.tcostdtl_id = '168' THEN 'SERVICE FEE FOR CERTIFIED TRUE COPY OF TCT'
											 WHEN v_TCost_Computation.tcostdtl_id = '180' THEN 'ANNOTATION OF SPA (IF APPLICABLE)'
											 WHEN v_TCost_Computation.tcostdtl_id = '219' THEN 'SERVICE FEE - SPA (IF APPLICABLE)'  ELSE  
									   (CASE WHEN v_TCost_Computation.tcostdtl_id = '038' THEN 'SERVICE FEE - RD' WHEN v_TCost_Computation.tcostdtl_id = '200' THEN 'CERTIFIED TRUE COPY OF TAX DECLARATION HOUSE' ELSE v_TCost_Computation.tcostdtl_desc end)end);

			IF  NULLIF(v_TCost_Computation.udf_name, '') is NULL THEN
			   c_tcost_detail_amt 	:= v_TCost_Computation.tcostdtl_amt; 
			ELSE
			raise info 'Compute Tcost';
			   c_tcost_detail_amt 	:= get_tcost_dtl_amt(v_TCost_Computation.tcostdtl_id, p_entity_id, p_proj_id, p_pbl_id, p_seq_no);
			END IF;

			c_applied_amt 			:= NULL; 
			c_remarks 				:= v_TCost_Computation.remarks;
			v_Total_Tcost_Amt 		:= COALESCE(v_Total_Tcost_Amt, 0.00) + COALESCE(c_tcost_detail_amt, 0.00);	

			RETURN NEXT;
			END LOOP;

			RAISE INFO 'Total TCost Amt: %', v_Total_Tcost_Amt;

			/**ADDED BY JED 2019-09-17 DCRF NO. 1216 : CHANGE NAME OF PARTICULAR**/
			FOR v_TCost_Computation IN (SELECT sum(tcostdtl_amt) as tcostdtl_amt, remarks
										FROM mf_transfer_cost_dl
												WHERE status_id = 'A' 
												AND   proj_id   = p_proj_id
												AND   for_tcostcomp is true
												AND   tcostdtl_id   in ('107', '017', '116', '065')
												AND   server_id     is NULL
										GROUP BY remarks
									   ) LOOP

			c_SELECT 				:= false; 
			c_tcost_detail_desc 	:= 'SERVICE FEE-ASSESSORS OFFICE';
			c_tcost_detail_amt 		:= v_TCost_Computation.tcostdtl_amt;
			c_applied_amt 			:= NULL; 
			c_remarks 				:= v_TCost_Computation.remarks;
			v_Total_Tcost_Amt 		:= COALESCE(v_Total_Tcost_Amt, 0.00) + COALESCE(c_tcost_detail_amt, 0.00);	

			RETURN NEXT;
			END LOOP;

			RAISE INFO 'Total TCost Amt: %', v_Total_Tcost_Amt;

				FOR v_TCost_Computation IN (SELECT * FROM mf_transfer_cost_dl
												WHERE status_id = 'A' 
												AND   proj_id     = p_proj_id
												AND   tcostdtl_id = '103'
												AND   for_tcostcomp is true
												AND   server_id is NULL
											ORDER BY  tcostdtl_desc) 
				LOOP

			c_SELECT 				:= false; 
			c_tcost_detail_desc 	:= v_TCost_Computation.tcostdtl_desc;
			c_tcost_detail_amt 		:= ROUND((v_Total_Tcost_Amt *.03), 2); 
			c_applied_amt 			:= NULL; 
			c_remarks 				:= v_TCost_Computation.remarks;

			RETURN NEXT;
			END LOOP;

			END IF;
	
	END IF;
END;
$BODY$;

ALTER FUNCTION public.view_card_tcost_computation_v2_debug(character varying, character varying, character varying, integer, boolean)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_card_tcost_computation_v2_debug(character varying, character varying, character varying, integer, boolean) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_card_tcost_computation_v2_debug(character varying, character varying, character varying, integer, boolean) TO employee;

GRANT EXECUTE ON FUNCTION public.view_card_tcost_computation_v2_debug(character varying, character varying, character varying, integer, boolean) TO postgres;

