DROP TABLE IF EXISTS data_update;

CREATE TEMP TABLE data_update (
  :(map
     (fn [column]
         (+ (get column 0) " " (get (get column 1) "psql_type")))
    columns),

  PRIMARY KEY (
    :primary_keys
  )
);


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
