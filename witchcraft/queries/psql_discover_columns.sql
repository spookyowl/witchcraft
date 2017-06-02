SELECT
  columns.column_name AS column_name,
  columns.data_type AS data_type,
  columns.character_maximum_length,
  COALESCE(columns.numeric_precision, 30) AS numeric_precision,
  COALESCE(columns.numeric_scale, 6) AS numeric_scale,
  columns.ordinal_position,
  (constraints.constraint_type IS NOT NULL) AS is_pkey

FROM information_schema.columns

LEFT JOIN (
  SELECT 
    table_constraints.constraint_type,
    table_constraints.constraint_schema,
    table_constraints.table_name,
    key_column_usage.column_name

  FROM information_schema.table_constraints

  LEFT JOIN information_schema.key_column_usage
    ON table_constraints.constraint_catalog = key_column_usage.constraint_catalog
    AND table_constraints.constraint_schema = key_column_usage.constraint_schema
    AND table_constraints.constraint_name = key_column_usage.constraint_name
    AND table_constraints.table_name = key_column_usage.table_name

  WHERE table_constraints.constraint_type = 'PRIMARY KEY'
) AS constraints
  ON constraints.constraint_schema = columns.table_schema
  AND constraints.table_name = columns.table_name
  AND constraints.column_name = columns.column_name

WHERE columns.table_name = ?table_name
  AND columns.table_schema = ?schema_name

ORDER BY ordinal_position;
