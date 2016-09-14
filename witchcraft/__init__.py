from witchcraft.upsert import prepare_table, upsert_data


def upsert(connection, schema_name, table_name, data_points, primary_keys):

    if len(data_points):
        return

    prepare_table(connection, schema_name, table_name, data_points, primary_keys)
    upsert_data(connection, schema_name, table_name, data_points, primary_keys)
