WITH tbl_prints AS (
  SELECT user_id, event_data.value_prop, event_data.position, day
  FROM `raw-zone-{environment}.ingestion_sources_mercadopago_{environment}.prints`
  WHERE day < DATE("{pivot_date}") AND day>=date_sub(DATE("{pivot_date}"), INTERVAL 4 WEEK) ##Pivot Date
),

prints_last_week AS (
  SELECT user_id, value_prop, position, day
  FROM tbl_prints
  WHERE day >= date_sub(DATE("{pivot_date}"), INTERVAL 1 WEEK) ##Pivot Date
),

prints_vp_3_week_previous AS (
  SELECT user_id, value_prop, position, count(*) AS q_prints_past
  FROM tbl_prints
  WHERE day < date_sub(DATE("{pivot_date}"), INTERVAL 1 WEEK) AND day >= date_sub(DATE("{pivot_date}"), INTERVAL 4 WEEK) ##Pivot Date
  GROUP BY user_id,value_prop, position
),

tbl_taps AS (
  SELECT user_id, event_data.value_prop, event_data.position, day
  FROM `raw-zone-{environment}.ingestion_sources_mercadopago_{environment}.taps`
  WHERE day < DATE("{pivot_date}") AND day >= date_sub(DATE("{pivot_date}"), INTERVAL 4 WEEK) ##Pivot Date
),

taps_last_week AS (
  SELECT user_id, value_prop, position, day
  FROM tbl_taps
  WHERE day >= date_sub(DATE("{pivot_date}"), INTERVAL 1 WEEK) ##Pivot Date
),

taps_vp_3_week_previous AS (
  SELECT user_id, value_prop, position, count(*) AS q_taps_past
  FROM tbl_taps
  WHERE day < date_sub(DATE("{pivot_date}"), INTERVAL 1 WEEK) AND day >= date_sub(DATE("{pivot_date}"), INTERVAL 4 WEEK) ##Pivot Date
  GROUP BY user_id,value_prop, position
),

pays_vp_3_week_previous AS (
  SELECT pays.user_id, pays.value_prop, COUNT(*) AS q_pays_past, SUM(pays.total) AS sum_pays_past
  FROM `raw-zone-{environment}.ingestion_sources_mercadopago_{environment}.pays` AS pays
  INNER JOIN tbl_taps AS taps ON taps.user_id=pays.user_id AND taps.value_prop=pays.value_prop and taps.day=pays.pay_date
  WHERE pays.pay_date < date_sub(DATE("{pivot_date}"), INTERVAL 1 WEEK) AND pays.pay_date >= date_sub(DATE("{pivot_date}"), INTERVAL 4 WEEK) ##Pivot Date
  AND taps.day < date_sub(DATE("{pivot_date}"), INTERVAL 1 WEEK) AND taps.day >= date_sub(DATE("{pivot_date}"), INTERVAL 4 WEEK)
  GROUP BY pays.user_id, pays.value_prop
),

use_case_carrusel AS (
  SELECT prt_lst_week.user_id, prt_lst_week.value_prop, prt_lst_week.position, prt_lst_week.day
        ,IF(tps_lst_week.day IS NULL, 0, 1)   AS bol_tap_same_day
        ,IFNULL(prt_vp_3_wp.q_prints_past,0)  AS q_prints_past
        ,IFNULL(clk_vp_3_wp.q_taps_past,0)    AS q_taps_past
        ,IFNULL(pys_vp_3_wp.q_pays_past,0)    AS q_pays_past
        ,IFNULL(pys_vp_3_wp.sum_pays_past,0)  AS sum_pays_past

  FROM prints_last_week               AS prt_lst_week

  LEFT JOIN taps_last_week            AS tps_lst_week USING (user_id,value_prop,position,day)
  LEFT JOIN prints_vp_3_week_previous AS prt_vp_3_wp  USING (user_id,value_prop,position)
  LEFT JOIN taps_vp_3_week_previous   AS clk_vp_3_wp  USING (user_id,value_prop,position)
  LEFT JOIN pays_vp_3_week_previous   AS pys_vp_3_wp  USING (user_id,value_prop)
)

SELECT user_id, value_prop, position, day, bol_tap_same_day, q_prints_past, q_taps_past, q_pays_past, sum_pays_past
FROM use_case_carrusel