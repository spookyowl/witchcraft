-- LOCK TABLE :schema_name.:table_name IN EXCLUSIVE MODE;
ANALYZE data_update;

WITH inserted_rows AS (

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
  
  WHERE :table_name.:(get primary_keys 0) IS NULL
  
  RETURNING 1
)
SELECT count(*) FROM inserted_rows;
