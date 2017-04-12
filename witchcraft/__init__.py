from witchcraft.utils import seekable
from witchcraft.upsert import prepare_table, upsert_data, insert_data
from witchcraft.upsert import delete_data, get_max_version
from witchcraft.combinators import query, template

#TODO: allow using serial primary key without defining value in data set
#TODO: combine schema_name + table_name into one argument

def upsert(connection, schema_name, table_name, data_points, primary_keys=None):
    #TODO: use seekable from utils and call prepare table (to create new columns) in batches (10000)
    data_points = list(data_points)

    if len(data_points) == 0:
        return

    if primary_keys is None:
        primary_keys = []

    primary_keys = prepare_table(connection, schema_name, table_name, data_points, primary_keys)
    upsert_data(connection, schema_name, table_name, data_points, primary_keys)


def insert(connection, schema_name, table_name, data_points, primary_keys):
    #TODO: use seekable from utils and call prepare table (to create new columns) in batches (10000)
    data_points = list(data_points)

    if len(data_points) == 0:
        return

    prepare_table(connection, schema_name, table_name, data_points, primary_keys)
    insert_data(connection, schema_name, table_name, data_points)


def replace(connection, schema_name, table_name, data_points):
    data_points = list(data_points)
    delete_data(connection, schema_name, table_name)
    prepare_table(connection, schema_name, table_name, data_points, [])
    insert_data(connection, schema_name, table_name, data_points)


def append_history(connection, schema_name, table_name, data_points):

    max_version = get_max_version(connection, schema_name, table_name)
    
    def add_history_column(item):
        item.add_field('version', psql_type='int')
        item['version'] = max_version
        return item  

    data_points = list(map(add_history_column, data_points))

    prepare_table(connection, schema_name, table_name, data_points, [])
    insert_data(connection, schema_name, table_name, data_points)
