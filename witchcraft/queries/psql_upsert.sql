CREATE TEMP TABLE data_update (
  :(map
     (fn [column]
         (+ (get column 0) " " (get (get column 1) "psql_type")))
    columns),

  PRIMARY KEY (
    :primary_keys
  )
);

ANALYZE data_update;

INSERT INTO data_update
  (:column_names)

VALUES ?(map
          (fn [dp]
            (list
             (map
                (fn [cn]
                   (get dp cn))
              column_names)))
           data-points)
;

-- LOCK TABLE :schema_name.:table_name IN EXCLUSIVE MODE;

UPDATE :schema_name.:table_name
  SET 
    :(map
   (fn [column] 
      (+ "\"" column "\"" "= data_update.\"" column "\""))
   column_names)

FROM data_update     
WHERE
1 = 1
:(reduce
   (fn [memo pkey]
     (+ memo " AND " schema-name "." table-name "." pkey "= data_update." pkey))
   primary_keys "")
;



INSERT INTO :schema_name.:table_name
  (:column_names)

SELECT
  :(map (fn [c] (+ "data_update." c)) column_names)

FROM data_update

LEFT OUTER JOIN :schema_name.:table_name 
ON 1 = 1
:(reduce
   (fn [memo pkey]
     (+ memo " AND " table-name "." pkey "= data_update." pkey))
   primary_keys "")

WHERE :table_name.:(get primary_keys 0) IS NULL;

DROP TABLE data_update;
