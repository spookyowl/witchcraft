INSERT INTO :schema_name.:table_name
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
