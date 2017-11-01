WITH inserted_rows AS (

  INSERT INTO :schema_name.:table_name
    (:column_names)
  
  VALUES ?(map
            (fn [dp]
              (list
               (map
                  (fn [cn]
                     (.get dp cn))
                column_names)))
             data-points)
  RETURNING 1
)
SELECT count(*) FROM inserted_rows;
