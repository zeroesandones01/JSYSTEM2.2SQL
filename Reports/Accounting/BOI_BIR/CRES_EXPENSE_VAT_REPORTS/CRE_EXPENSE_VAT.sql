
SELECT *
FROM view_input_vat_debug('02', '', '2024-01-01', '2024-01-31')
WHERE c_vat::numeric(19, 2) != 0::numeric(19, 2)
ORDER BY c_goods_services;

select TRIM(replace('PV 000100273', 'PV', ''));

