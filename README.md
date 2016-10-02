(query )

(sql-tpl )

(filter )

(filter-by )

TODO:
 - zisti ako to ma byt s tym intersect ako ma zachovavat mena klucov atd.
 - implementuj v select kombinatore premenovavanie klucov
 - sprav TupleSet a TupleDict a obom daj vestky aplikovatelne kobinatory ako member
   funkcie a boli z nich monady
 - sprav prototyp "serialize" funkcie/kombinatora pre zaciatok do db (upsert_data)
    -> mode upsert/replace/no_op
    -> vylepsi hashing algoritmus pri upserte tak aby bolo mozne pridavat stplce do primarneho kluca
    -> neskor ak bude usecase do suboru yaml/json/csv
 - nejako zjednot implementaciu Tuple a DictItem
 - zredukuj zoznam rezervovanych slov pre postgres a zbav sa toho textaca
 - pories podporu ostatnych databaz (backbport z dirac)
