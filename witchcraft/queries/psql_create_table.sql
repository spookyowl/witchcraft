CREATE TABLE :schema_name.:table_name (
  :(map
     (fn [column]
         (+ (get column 0) " " (get (get column 1) "psql_type")))
    columns),

  PRIMARY KEY (
    :primary_keys
  )
);
