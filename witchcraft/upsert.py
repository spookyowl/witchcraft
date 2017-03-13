from witchcraft.combinators import execute, query, template


prefix_dict = {                                                                    
    'pgsql': 'psql',                                                            
    'mysql': 'mysql',                                                              
    'oracle': 'oracle',                                                            
    'mssql': 'mssql',                                                              
}
     

def find_keys(dps):                                                                
    keys = set()

    for i in dps:
        keys |= set(i.keys())

    return keys


def create_table(connection, schema_name, table_name, fields, primary_keys):

    columns = fields.items()
    prefix = prefix_dict.get(connection.database_type)
    execute(connection, template('%s_create_table' % prefix, 
                            dict(columns=columns,
                                 schema_name=schema_name,
                                 table_name=table_name,
                                 primary_keys=primary_keys)))


def prepare_table(connection, schema_name, table_name, data_points, primary_keys):

    fields = data_points[0].fields
    prefix = prefix_dict.get(connection.database_type)

    required_columns = find_keys(data_points)
    discovery_table_name = table_name

    if connection.database_type == 'oracle':
        discovery_table_name = table_name.upper()

    result = query(connection, template('%s_discover_columns' % prefix,
                                     dict(schema_name=schema_name,
                                          table_name=discovery_table_name)))

    discovered_columns = set(map(lambda r: r.column_name.lower(), result))
    
    if len(discovered_columns) == 0:
        create_table(connection, schema_name, table_name, fields, primary_keys)

        result = query(connection, template('%s_discover_columns' % prefix,
                                     dict(schema_name=schema_name,
                                          table_name=discovery_table_name)))

        discovered_columns = set(map(lambda r: r.column_name.lower(), result))


    else:
        discovered_pkeys = filter(lambda r: r.is_pkey, result)
        discovered_pkeys = map(lambda r: r.column_name.lower(), discovered_pkeys)
        if set(primary_keys) != set(discovered_pkeys):
            raise ValueError('Primary keys in destination table are not matching')

    missing_columns = required_columns - discovered_columns

    for column_name in missing_columns:
        column_type = fields.get(column_name)['%s_type' % prefix]

        execute(connection, template('%s_add_column' % prefix,
                                dict(schema_name=schema_name,
                                     table_name=table_name,
                                     column_name=column_name,
                                     column_type=column_type)))


def upsert_data(connection, schema_name, table_name, data_points, primary_keys):

    prefix = prefix_dict.get(connection.database_type)
    column_names = list(find_keys(data_points))

    execute(connection, template('%s_upsert' % prefix,
                            dict(schema_name=schema_name,
                                 table_name=table_name,
                                 column_names=column_names,
                                 columns=data_points[0].fields.items(),
                                 data_points=data_points,
                                 primary_keys=primary_keys),
                            connection.database_type))


def insert_data(connection, schema_name, table_name, data_points):
    prefix = prefix_dict.get(connection.database_type)
    column_names = list(find_keys(data_points))

    execute(connection, template('%s_insert' % prefix,
                            dict(schema_name=schema_name,
                                 table_name=table_name,
                                 column_names=column_names,
                                 columns=data_points[0].fields.items(),
                                 data_points=data_points),
                            connection.database_type))
