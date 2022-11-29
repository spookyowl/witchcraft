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
;
