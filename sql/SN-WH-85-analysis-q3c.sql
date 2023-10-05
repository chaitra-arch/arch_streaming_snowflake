-- COST Part Ends
SELECT wh_and_size, warehouse_size, 
SAVING_CREDITS_SIZE_DOWN_1MINBKT as POSSIBLE_SAVING_CREDITS_ON_1MIN_BKT
,SAVING_CREDITS_SIZE_DOWN
--,CALC_SAVING
FROM 
(
    select mq.wh_and_size, --WAREHOUSE_NAME, warehouse_size,
    mq.warehouse_size,
    ROUND(min(SAVING_CREDITS_SIZE_DOWN_1minBKT), 2) as SAVING_CREDITS_SIZE_DOWN_1minBKT,
    ROUND(min(SAVING_CREDITS_SIZE_DOWN), 2)         as SAVING_CREDITS_SIZE_DOWN,
    count(*) as cnt_recommendations,
    REPLACE(REPLACE(to_char(ARRAY_AGG(TRIM(RULE_OUTPUT1)) WITHIN GROUP (ORDER BY mq.WAREHOUSE_NAME ASC)),'[','') ,']','') as recommendations,
    --TRIM(RULE_OUTPUT1) as RECOMMEND_OUTPUT --, QS_PER_TIME as QS_PER_TIME1
    --, * 
    CASE 
        WHEN (recommendations LIKE '%5.%' or recommendations LIKE '%1.%' or recommendations LIKE '%2.%') THEN 1 ELSE 0
    END as CALC_SAVING
    --,AVG(SAVING_CREDITS_SIZE_DOWN_1minBKT * CALC_SAVING) as POSSIBLE_SAVING
    FROM {db_name}.{sc_name}.MONITOR_AND_ANALYS_WH_STEP_TWO mq--IDRC_SBX_IDROM.CMS_WORK_COMM_PRD.MONITOR_AND_ANALYS_WH_STEP_TWO mq
    JOIN {db_name}.{sc_name}.MONITOR_AND_ANALYS_WH_STEP_THREE_A --IDRC_SBX_IDROM.CMS_WORK_COMM_PRD.MONITOR_AND_ANALYS_WH_STEP_THREE_A  --CTE_CREDIT_RESULT 
        cr ON (mq.warehouse_name = cr.warehouse_name AND mq.warehouse_size = cr.warehouse_size)
    JOIN {db_name}.{sc_name}.MONITOR_AND_ANALYS_WH_STEP_THREE_B -- IDRC_SBX_IDROM.CMS_WORK_COMM_PRD.MONITOR_AND_ANALYS_WH_STEP_THREE_B  --CTE_CREDIT_RESULT 
        cr1 ON (mq.warehouse_name = cr1.warehouse_name AND mq.warehouse_size = cr1.warehouse_size)
    WHERE 
    TRIM(RULE_OUTPUT1) <> '' 
    --and warehouse_name like '%ACO%';
    group by mq.wh_and_size, mq.warehouse_size --,warehouse_size 
    HAVING  CALC_SAVING = 1
    ORDER BY SAVING_CREDITS_SIZE_DOWN_1minBKT DESC
)
--WHERE POSSIBLE_SAVING_CREDITS_ON_1MIN_BKT > 100 -- Filtering for a relatively small saving?
order by POSSIBLE_SAVING_CREDITS_ON_1MIN_BKT DESC, wh_and_size asc;
