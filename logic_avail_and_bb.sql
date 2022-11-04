use proj_web2becomm;

drop table if exists tmp_availability_with_comms;
create table tmp_availability_with_comms as (
  select
    rav.*,
    atc.comms as comms_number
  from
    proj_web2becomm.stg_estore_daily_restricted_availability_buy_box rav
  left join
    proj_web2becomm.dim_asin_lookup atc
    on
      rav.asin = atc.asin
);

drop table if exists tmp_sioc_aaa;
create table tmp_sioc_aaa AS (
  SELECT
    `date`,
    iso_week_year,
    iso_week,
    country_code,
    asin,
    comms_number,
    sioc_flag AS sioc_flag_sioc,
    market_flag AS market_flag_sioc,
    metric_buy_box_win_percent AS bb_win_pct_sioc
  FROM tmp_availability_with_comms
  WHERE
    sioc_flag = true
  ORDER BY
    `date`,
    country_code,
    comms_number
);


drop table if exists tmp_non_sioc_aaa;
create table tmp_non_sioc_aaa AS (
  SELECT
    `date`,
    iso_week_year,
    iso_week,
    country_code,
    asin,
    comms_number,
    sioc_flag AS sioc_flag_non_sioc,
    market_flag AS market_flag_non_sioc,
    metric_buy_box_win_percent AS bb_win_pct_non_sioc
  FROM tmp_availability_with_comms
  WHERE
    sioc_flag = false
  ORDER BY
    `date`,
    country_code,
    comms_number
);


drop table if exists stg_availability_lego_logic_table;
create table stg_availability_lego_logic_table AS
  WITH sioc_non_sioc_view AS (
  SELECT
    coalesce(nsc.`date`, ysc.`date`) as `date`,
    coalesce(nsc.iso_week_year, ysc.iso_week_year) as iso_week_year,
    coalesce(nsc.iso_week, ysc.iso_week) as iso_week,
    coalesce(nsc.country_code, ysc.country_code) as country_code,
    coalesce(nsc.comms_number,  ysc.comms_number) as comms_number,
    nsc.asin as asin,
    ysc.asin as sioc_asin,
    nsc.sioc_flag_non_sioc,
    ysc.sioc_flag_sioc,
    nsc.market_flag_non_sioc,
    ysc.market_flag_sioc,
    nsc.bb_win_pct_non_sioc,
    ysc.bb_win_pct_sioc
  FROM
    tmp_non_sioc_aaa nsc
  FULL JOIN
    tmp_sioc_aaa ysc
    ON
      nsc.`date` = ysc.`date`
    AND
      nsc.country_code = ysc.country_code
    AND
      nsc.comms_number = ysc.comms_number
),
  sioc_non_sioc_view_with_logic AS (
  SELECT
    *,
    CASE
                WHEN (market_flag_non_sioc = 'available' AND bb_win_pct_non_sioc = 100) OR (market_flag_sioc = 'available' AND bb_win_pct_sioc = 100) THEN 'available'
                WHEN (market_flag_non_sioc = 'available' AND bb_win_pct_non_sioc < 100) OR (market_flag_sioc = 'available' AND bb_win_pct_sioc < 100) THEN 'lost buy-box'
                WHEN market_flag_non_sioc IN ('partial_oos', 'absolute_oos') OR market_flag_sioc IN ('partial_oos', 'absolute_oos') THEN 'oos'
                WHEN market_flag_non_sioc IN ('partial_crap', 'absolute_crap') OR market_flag_sioc IN ('partial_crap', 'absolute_crap') THEN 'crap'
                ELSE NULL
    END AS lego_logic_market_flag,
    CASE
        WHEN bb_win_pct_non_sioc >= bb_win_pct_sioc OR bb_win_pct_sioc IS NULL THEN bb_win_pct_non_sioc
        WHEN bb_win_pct_non_sioc < bb_win_pct_sioc  OR bb_win_pct_non_sioc IS NULL THEN bb_win_pct_sioc
      ELSE NULL
    END AS lego_logic_bb_win_pct
  FROM
    sioc_non_sioc_view
  where     asin is not null or sioc_asin is not null
  ORDER BY
    `date`,
    iso_week,
    country_code,
    comms_number
),
   lego_logic_table_final AS (
  SELECT
    `date`,
    iso_week_year,
    iso_week,
    country_code,
    comms_number,
    asin,
    sioc_asin,
    CASE
      WHEN sioc_asin is not null THEN true
      ELSE NULL
    END AS comms_has_sioc_flag,
    market_flag_non_sioc,
    market_flag_sioc,
    lego_logic_market_flag,
    bb_win_pct_non_sioc,
    bb_win_pct_sioc,
    lego_logic_bb_win_pct
  FROM
    sioc_non_sioc_view_with_logic
  ORDER BY
    'date',
    iso_week,
    country_code
)
select * from lego_logic_table_final
WHERE iso_week_year = 2022
;


drop table if exists proj_web2becomm.pl_amazon_availability;
create table proj_web2becomm.pl_amazon_availability as

with asin_union as (
select `date`,
country_code,
comms_number,
coalesce(asin, sioc_asin) as asin,
lego_logic_market_flag as market_flag,
comms_has_sioc_flag,
'All' as assortment
from stg_availability_lego_logic_table
where lego_logic_market_flag is not null

union all

select `date`,
country_code,
comms_number,
sioc_asin as asin,
case when market_flag_sioc like '%crap%' then 'crap'
when market_flag_sioc like '%oos%' then 'oos'
else 'available' end as market_flag,
comms_has_sioc_flag,
'SIOC' as assortment
from stg_availability_lego_logic_table
where market_flag_sioc is not null

union all

select `date`,
country_code,
comms_number,
asin,
case when market_flag_non_sioc like '%crap%' then 'crap'
when market_flag_non_sioc like '%oos%' then 'oos'
else 'available' end as market_flag,
comms_has_sioc_flag,
'Non SIOC' as assortment
from stg_availability_lego_logic_table
where market_flag_non_sioc is not null)


select a.*,
      -- b.material_no,
	   cast(b.comms as string) || "-" || cast(c.iso_week_year as string) as comm_year,
       cast(b.material_no as string) || "-" || cast(c.iso_week_year  as string) as material_year
       from asin_union a
left join dim_asin_lookup b
	on b.asin = a.asin
left join dim_calendar c
on a.`date` = c.calendar_date
;



-- Clean up temp tables:

drop table if exists tmp_availability_with_comms;
drop table if exists tmp_sioc_aaa;
drop table if exists tmp_non_sioc_aaa;

-- load check

select `date`, count(*) from pl_amazon_availability
group by 1
order by 1 desc;

