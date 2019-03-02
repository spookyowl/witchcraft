UPDATE :schema_name.:table_name
  SET 
    :(map
   (fn [column] 
      (+ "\"" column "\"" "= data_update.\"" column "\""))
   column_names),
   _updated_at = now()

FROM data_update     
WHERE
1 = 1
:(reduce
   (fn [memo pkey]
     (+ memo " AND " schema-name "." table-name "." pkey "= data_update." pkey))
   primary_keys "")

;
