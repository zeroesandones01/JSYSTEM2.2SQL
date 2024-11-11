--select * from view_sched_cashflow_details_with_proj_phase($P{co_id},$P{proj_id},$P{phase_no},$P{date_from},$P{date_to})

select * from view_sched_cashflow_details_with_proj_phase('01','','','2024-10-01','2024-10-30') where c_pv_no = '000013883';

