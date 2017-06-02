from collections import namedtuple
from witchcraft.utils import seekable
from witchcraft.upsert import prepare_table, upsert_data, insert_data
from witchcraft.upsert import delete_data, get_max_version
from witchcraft.combinators import query, template


LoadResult = ('LoadResult', ['received','inserted', 'updated', 'deleted', 'update_started_at', 'finished_at'])


#TODO: allow using serial primary key without defining value in data set - solves: e.g. writing crawling/scraping results
#TODO: combine schema_name + table_name into one argument

def upsert(connection, schema_name, table_name, data_points, primary_keys=None):
    #TODO: use seekable from utils and call prepare table (to create new columns) in batches (10000)
    data_points = list(data_points)

    if len(data_points) == 0:
        return

    if primary_keys is None:
        primary_keys = []
    
    primary_keys = prepare_table(connection, schema_name, table_name, data_points, primary_keys)

    if len(primary_keys) == 0:
        raise ValueError('Upsert method requires table to have primary keys')

    update_started_at = connection.get_current_timestamp()
    inserted, updated = upsert_data(connection, schema_name, table_name, data_points, primary_keys)
    finished_at = connection.get_current_timestamp()

    return LoadResult(len(data_points), inserted, updated, 0, update_started_at, finished_at)


def insert(connection, schema_name, table_name, data_points, primary_keys):
    #TODO: use seekable from utils and call prepare table (to create new columns) in batches (10000)
    data_points = list(data_points)

    if len(data_points) == 0:
        return

    prepare_table(connection, schema_name, table_name, data_points, primary_keys)
    update_started_at = connection.get_current_timestamp()
    inserted = insert_data(connection, schema_name, table_name, data_points)
    finished_at = connection.get_current_timestamp()

    return LoadResult(len(data_points), inserted, 0, 0, update_started_at, finished_at)


def replace(connection, schema_name, table_name, data_points, primary_keys=None):
    if primary_keys is None:
        primary_keys = []
        
    data_points = list(data_points)
    prepare_table(connection, schema_name, table_name, data_points, primary_keys)

    update_started_at = connection.get_current_timestamp()
    deleted = delete_data(connection, schema_name, table_name)
    inserted = insert_data(connection, schema_name, table_name, data_points)
    finished_at = connection.get_current_timestamp()

    return LoadResult(len(data_points), inserted, 0, deleted, update_started_at, finished_at)


def append_history(connection, schema_name, table_name, data_points, primary_keys=None):

    max_version = get_max_version(connection, schema_name, table_name)
    
    def add_history_column(item):
        item.add_field('version', psql_type='int')
        item['version'] = max_version
        return item  

    data_points = list(map(add_history_column, data_points))

    if primary_keys is None:
        primary_keys = []

    prepare_table(connection, schema_name, table_name, data_points, primary_keys)

    update_started_at = connection.get_current_timestamp()
    inserted = insert_data(connection, schema_name, table_name, data_points)
    finished_at = connection.get_current_timestamp()

    return LoadResult(len(data_points), inserted, 0, 0, update_started_at, finished_at)
