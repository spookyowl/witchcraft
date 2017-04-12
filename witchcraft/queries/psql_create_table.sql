CREATE TABLE :schema_name.:table_name (
  :(map
     (fn [column]
         (+ (get column 0) " " (get (get column 1) "psql_type")))
    columns),
  _created_at timestamp without time zone not null default localtimestamp,
  _updated_at timestamp without time zone not null default localtimestamp

  :(if (> (len primary_keys) 0)
    (+ ", PRIMARY KEY (" (.join "," primary_keys) ")"))
);
