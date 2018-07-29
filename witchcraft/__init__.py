from collections import namedtuple
from datetime import datetime
from witchcraft.utils import read_batch, remove_null_rows
from witchcraft.upsert import prepare_table, upsert_data, insert_data
from witchcraft.upsert import delete_data, get_max_version
from witchcraft.combinators import query, template


LoadResult = namedtuple('LoadResult', ['received', 'inserted', 'updated', 'deleted', 'update_started_at', 'finished_at'])

#TODO: allow using serial primary key without defining value in data set - solves: e.g. writing crawling/scraping results
#TODO: combine schema_name + table_name into one argument

def upsert(connection, schema_name, table_name, data_points, primary_keys=None):
    read_total = 0
    inserted_total = 0
    updated_total = 0

    if isinstance(data_points, list):
        iterator = iter(data_points)
    else:
        iterator = data_points

    first_batch = read_batch(iterator)

    if len(first_batch) == 0:
        now = connection.get_current_timestamp()
        return LoadResult(0, 0, 0, 0, now, now)

    first_batch = remove_null_rows(first_batch, primary_keys)

    if primary_keys is None:
        primary_keys = []

    primary_keys = prepare_table(connection, schema_name, table_name, first_batch, primary_keys)

    if len(primary_keys) == 0:
        raise ValueError('Upsert method requires table to have primary keys')

    update_started_at = connection.get_current_timestamp()

    inserted, updated = upsert_data(connection, schema_name, table_name, first_batch, primary_keys)
    read_total += len(first_batch)
    inserted_total += inserted
    updated_total += updated_total

    while True:
        batch = read_batch(iterator)
        batch = remove_null_rows(batch, primary_keys)

        if len(batch) > 0:
            primary_keys = prepare_table(connection, schema_name, table_name, batch, primary_keys)
            inserted, updated = upsert_data(connection, schema_name, table_name, batch, primary_keys)
            read_total += len(batch)
            inserted_total += inserted
            updated_total += updated_total

        else:
            break

    finished_at = connection.get_current_timestamp()

    return LoadResult(read_total, inserted_total, updated_total, 0, update_started_at, finished_at)


def insert(connection, schema_name, table_name, data_points, primary_keys):
    read_total = 0
    inserted_total = 0

    if isinstance(data_points, list):
        iterator = iter(data_points)
    else:
        iterator = data_points

    first_batch = read_batch(iterator)

    if len(first_batch) == 0:
        now = connection.get_current_timestamp()
        return LoadResult(0, 0, 0, 0, now, now)

    first_batch = remove_null_rows(first_batch, primary_keys)

    prepare_table(connection, schema_name, table_name, first_batch, primary_keys)
    update_started_at = connection.get_current_timestamp()

    inserted_total += insert_data(connection, schema_name, table_name, first_batch)
    read_total += len(first_batch)

    while True:
        batch = read_batch(iterator)
        batch = remove_null_rows(batch, primary_keys)

        if len(batch) > 0:
            prepare_table(connection, schema_name, table_name, batch, primary_keys)
            inserted_total += insert_data(connection, schema_name, table_name, batch)
            read_total += len(batch)
        else:
            break

    finished_at = connection.get_current_timestamp()

    return LoadResult(read_total, inserted_total, 0, 0, update_started_at, finished_at)


def replace(connection, schema_name, table_name, data_points, primary_keys=None):
    read_total = 0
    inserted_total = 0
    deleted_total = 0

    if primary_keys is None:
        primary_keys = []
     
    if isinstance(data_points, list):
        iterator = iter(data_points)
    else:
        iterator = data_points

    first_batch = read_batch(iterator)

    if len(first_batch) == 0:
        now = connection.get_current_timestamp()
        return LoadResult(0, 0, 0, 0, now, now)

    first_batch = remove_null_rows(first_batch, primary_keys)

    prepare_table(connection, schema_name, table_name, first_batch, primary_keys)

    update_started_at = connection.get_current_timestamp()
    deleted_total = delete_data(connection, schema_name, table_name)
    inserted_total += insert_data(connection, schema_name, table_name, first_batch)
    read_total += len(first_batch)

    while True:
        batch = read_batch(iterator)
        batch = remove_null_rows(batch, primary_keys)

        if len(batch) > 0:
            prepare_table(connection, schema_name, table_name, batch, primary_keys)
            inserted_total += insert_data(connection, schema_name, table_name, batch)
            read_total += len(batch)
        else:
            break

    finished_at = connection.get_current_timestamp()
    return LoadResult(read_total, inserted_total, 0, deleted_total, update_started_at, finished_at)


def append_history(connection, schema_name, table_name, data_points, primary_keys=None):
    read_total = 0
    inserted_total = 0
    max_version = get_max_version(connection, schema_name, table_name)

    def add_history_column(item):
        item.add_field('version', psql_type='int')
        item['version'] = max_version
        return item  

    if isinstance(data_points, list):
        iterator = iter(data_points)
    else:
        iterator = data_points

    if primary_keys is None:
        primary_keys = []

    first_batch = read_batch(iterator)

    first_batch = remove_null_rows(first_batch, primary_keys)

    first_batch = list(map(add_history_column, first_batch))

    if len(first_batch) == 0:
        now = connection.get_current_timestamp()
        return LoadResult(0, 0, 0, 0, now, now)


    prepare_table(connection, schema_name, table_name, first_batch, primary_keys)

    update_started_at = connection.get_current_timestamp()
    inserted_total += insert_data(connection, schema_name, table_name, first_batch)
    read_total += len(first_batch)

    while True:
        batch = read_batch(iterator)
        batch = remove_null_rows(batch, primary_keys)
        batch = list(map(add_history_column, batch))

        if len(batch) > 0:
            prepare_table(connection, schema_name, table_name, batch, primary_keys)
            inserted_total += insert_data(connection, schema_name, table_name, batch)
            read_total += len(batch)
        else:
            break


    finished_at = connection.get_current_timestamp()

    return LoadResult(read_total, inserted_total, 0, 0, update_started_at, finished_at)
