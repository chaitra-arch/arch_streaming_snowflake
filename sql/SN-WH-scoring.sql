--  Name:         SN-WH-scoring.sql  / SN-WH-scoring.sql 
--  Created Date: 11-May-2023
--  Description:  https://snowflakecomputing.atlassian.net/wiki/spaces/SKE/pages/2323415216/Health+check+of+Compute+-+Customer+Account
-- Warehouse scoring
------------------------------------------------------------------------------------------

-- Warehouse scoring last 30 days
with wh_metering as (
    select
        warehouse_name,
        round(sum(credits_used_compute), 0) as compute_credits_used,
        round(sum(credits_used_cloud_services), 0) as cloud_services_credits_used
    from
        snowflake.account_usage.warehouse_metering_history
    where
        start_time >= dateadd('days', -30, current_date())
    group by 1
),

wh_kpis as (
    select
        job.warehouse_name,
        job.warehouse_size,
        avg(query_load_percent) as avg_pct_warehouse_used,
        percentile_cont(.90) within group (
            order by
                (query_load_percent)
        ) as p90_wh_used,
        count(distinct job.query_id) as job_count,
        sum(job.queued_overload_time) / 1000 as total_queue_time,
        sum(total_elapsed_time) / 1000 as total_duration,
        avg(queued_overload_time / total_elapsed_time) as avg_pct_total_queued,
        avg(execution_time) / 1000 as avg_xp_duration,
        avg(total_elapsed_time) / 1000 as avg_total_dur,
        avg(bytes_spilled_to_remote_storage) / power(1024, 3) as remote_temp_space_usage_gb,
        sum(case when bytes_spilled_to_remote_storage > 0 then 1 else 0 end) as num_jobs_remote_spilling,
        sum(case when bytes_spilled_to_remote_storage > 0 then 1 else 0 end) / job_count as pct_jobs_spilled_remote
    from
        snowflake.account_usage.query_history job
    where
        1 = 1
        and job.cluster_number is not null
        and job.start_time >= dateadd('days', -30, current_date()) --BETWEEN '{start_date}' AND '{end_date}'
    group by 1, 2
),

cat_scores as (
    select
        warehouse_name,
        warehouse_size,
        case when p90_wh_used < 100 then 100::number end as p90_wh_used_score,  -- if 90 percent of the jobs dont use the full warehosue, pad the score to the to small side
        (100 - (avg_pct_warehouse_used))::number as avg_wh_used_score,  -- add one point for each avg pct below 100 used
        case
            when avg_xp_duration < 10 then ((60 - avg_xp_duration) * 2) :: number
            else (60 - avg_xp_duration)::number
        end as xp_dur_score,  --subtract a point for each second over 60, add a point for each second below 60
        -(pct_jobs_spilled_remote * 100 * 2)::number as pct_jobs_spilling_score --add two points for each percentage of jobs with spilling to remote
    from
        wh_kpis
)

select
    piv.warehouse_name,
    piv.warehouse_size,
    sum(piv.score) as warehouse_size_score,
    case
        when abs(warehouse_size_score) > 300 then 'attention: immediate action recommended'
        when abs(warehouse_size_score) <= 100 then 'no action necessary'
        else 'warning: potential action required, continue to monitor and assess'
    end necessary_action,
    case
        when warehouse_size_score > 0 then 'big'
        else 'small'
    end size_scale,
    wh_metering.compute_credits_used
    --,necessary_action || '.  warehouse ' || piv.warehouse_name || ' has a score of ' || warehouse_size_score || ' and might be too ' || size_scale || ' (' || compute_credits_used || ' credits consumed over past 30 days)' insight
from
    cat_scores unpivot(score for scores in (p90_wh_used_score, avg_wh_used_score, xp_dur_score, pct_jobs_spilling_score)) as piv
left join wh_metering 
    on piv.warehouse_name = wh_metering.warehouse_name
group by
    piv.warehouse_name,
    piv.warehouse_size,
    wh_metering.compute_credits_used
order by
    abs(warehouse_size_score) desc

    limit 30
;