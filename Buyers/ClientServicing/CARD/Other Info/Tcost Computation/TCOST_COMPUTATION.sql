SELECT * FROM view_card_tcost_computation_v2_debug('5488771214', '015', '666', 1, true); --CAVITE WITH HOUSE
SELECT get_client_name('0629288911'), * FROM view_card_tcost_computation_v2_debug('0629288911', '015', '3996', 1, true); --CAVITE LOT
SELECT * FROM view_card_tcost_computation_v2_debug('9676179990', '003', '7061', 5, true); --MONTALBAN LOT
SELECT * FROM view_card_tcost_computation_v2_debug('5705118161', '003', '6682', 2, true); --MONTALBAN LOT


SELECT * 
FROM mf_transfer_cost_dl
WHERE status_id = 'A' 
AND proj_id = '015'
and case when '017' IN ('009', '017') THEN tcostdtl_id IN ('263', '262', '269', '260', '258', '254', '261', '103', '265', '268', '256') 
		ELSE tcostdtl_id in ('263', '262', '260', '258', '254', '261', '103', '265', '268', '256') END
ORDER BY tcostdtl_desc;

select * from mf_transfer_cost_dl WHERE tcostdtl_id = '256';

select * from mf_product_model where model_desc ~*'LOT' and status_id = 'A';

SELECT get_client_name(entity_id), * FROM rf_sold_unit where projcode != '015' and trim(model_id) = '017';

SELECT * FROM mf_product_model where model_id = '017' and proj_server = 'cenq_eb';

SELECT server_id, proj_server, * FROM rf_sold_unit where entity_id = '0505802378';

select * from rf_sold_unit where model_id = '160';