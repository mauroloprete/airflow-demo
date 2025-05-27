WITH latest_success AS (
  SELECT
    id_entity,
    MAX(TO_TIMESTAMP_NTZ(CAST(date AS STRING), 'yyyyMMdd')) AS last_success_date
  FROM dev_ref_control.general.ref_log_execs
  WHERE status = 'success'
  GROUP BY id_entity
),
to_run AS (
  SELECT 
    en.id_entity,
    en.grouper_name,
    en.layer_name,
    en.container_name,
    en.entity_name,
    en.defined_calendar,
    par.parameter_name as parameters,
    en.activation_state,
    ls.last_success_date
  FROM dev_ref_control.general.ref_master_entities en
  INNER JOIN dev_ref_control.general.ref_master_parameters par 
    ON par.id_entity = en.id_entity
  LEFT JOIN dev_ref_control.general.ref_relation_ingest_connection relcon 
    ON relcon.id_entity = en.id_entity
  LEFT JOIN dev_ref_control.general.ref_connections con 
    ON con.id_connection = relcon.id_connection
  LEFT JOIN latest_success ls 
    ON ls.id_entity = en.id_entity
  WHERE en.active_state = 'Y'
)
SELECT *
FROM to_run
WHERE (
  (defined_calendar = 'hourly' AND (last_success_date IS NULL OR DATE_TRUNC('HOUR', last_success_date) < TRUNC(CURRENT_TIMESTAMP, 'HOUR')))
  OR
  (defined_calendar = 'daily' AND (last_success_date IS NULL OR DATE(last_success_date) < CURRENT_DATE))
  OR
  (defined_calendar = 'monthly' AND (last_success_date IS NULL OR DATE_TRUNC('MONTH', last_success_date) < DATE_TRUNC('MONTH', CURRENT_DATE)))
  OR
  (defined_calendar = 'quarterly' AND (last_success_date IS NULL OR DATE_TRUNC('QUARTER', last_success_date) < DATE_TRUNC('QUARTER', CURRENT_DATE)))
  OR
  (defined_calendar = 'yearly' AND (last_success_date IS NULL OR DATE_TRUNC('YEAR', last_success_date) < DATE_TRUNC('YEAR', CURRENT_DATE)))
)
