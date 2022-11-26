DELETE FROM data_update;

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
