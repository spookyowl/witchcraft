# -*- coding: utf-8 -*-
import string
from decimal import Decimal
import csv
import re
from witchcraft.combinators import execute, query, template
from witchcraft.dateutil.parser import parse as dateutil_parse


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


#TODO: get rid of side effect
def remove_metadata(data_points):
    
    for item in data_points:
        if '_updated_at' in item:
            del item['_updated_at']

        if '_created_at' in item:
            del item['_created_at']


def create_table(connection, schema_name, table_name, fields, primary_keys):

    columns = fields.items()
    prefix = prefix_dict.get(connection.database_type)
    execute(connection, template('%s_create_table' % prefix, 
                            dict(columns=columns,
                                 schema_name=schema_name,
                                 table_name=table_name,
                                 primary_keys=primary_keys)))

# TODO: implement column upgrade/add as we go through data points To make this 
# possible DataPoint have to implement simple way of checking if fields sqltype changed.
# 

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
        discovered_pkeys = list(map(lambda r: r.column_name.lower(), discovered_pkeys))

        if len(primary_keys) != 0:

            if set(primary_keys) != set(discovered_pkeys):
                raise ValueError('Primary keys in destination table are not matching with defined primary keys')
            primary_keys = discovered_pkeys

    missing_columns = required_columns - discovered_columns

    for column_name in missing_columns:
        column_type = fields.get(column_name)['%s_type' % prefix]

        execute(connection, template('%s_add_column' % prefix,
                                dict(schema_name=schema_name,
                                     table_name=table_name,
                                     column_name=column_name,
                                     column_type=column_type)))
    return primary_keys


def upsert_data(connection, schema_name, table_name, data_points, primary_keys):

    prefix = prefix_dict.get(connection.database_type)
    column_names = list(find_keys(data_points))
    connection.begin()

    execute(connection, template('%s_upsert_load' % prefix,
                        dict(schema_name=schema_name,
                             table_name=table_name,
                             column_names=column_names,
                             columns=data_points[0].fields.items(),
                             data_points=data_points,
                             primary_keys=primary_keys),
                        connection.database_type))

    updated = execute(connection, template('%s_upsert_update' % prefix,
                        dict(schema_name=schema_name,
                             table_name=table_name,
                             column_names=column_names,
                             columns=data_points[0].fields.items(),
                             data_points=data_points,
                             primary_keys=primary_keys),
                        connection.database_type))

    inserted = query(connection, template('%s_upsert_insert' % prefix,
                        dict(schema_name=schema_name,
                             table_name=table_name,
                             column_names=column_names,
                             columns=data_points[0].fields.items(),
                             data_points=data_points,
                             primary_keys=primary_keys),
                        connection.database_type))
    connection.commit()

    return (inserted[0].count, updated)


def insert_data(connection, schema_name, table_name, data_points):
    prefix = prefix_dict.get(connection.database_type)
    column_names = list(find_keys(data_points))

    inserted = query(connection, 
                   template('%s_insert' % prefix,
                            dict(schema_name=schema_name,
                                 table_name=table_name,
                                 column_names=column_names,
                                 columns=data_points[0].fields.items(),
                                 data_points=data_points),
                            connection.database_type))

    connection.commit()
    return inserted[0].count


def delete_data(connection, schema_name, table_name):
    prefix = prefix_dict.get(connection.database_type)

    result = query(connection, template('%s_discover_columns' % prefix,
                                        dict(schema_name=schema_name,
                                             table_name=table_name)))

    if len(result) == 0:
        return 0


    #TODO: use SqlSession.delete directly?
    deleted_count = execute(connection,
                            template('%s_delete' % prefix,
                                     dict(schema_name=schema_name,
                                          table_name=table_name),
                                          connection.database_type))

    return deleted_count
   

def discover_columns(connection, schema_name, table_name):
    prefix = prefix_dict.get(connection.database_type)
    result = query(connection, template('%s_discover_columns' % prefix,
                                 dict(schema_name=schema_name,
                                      table_name=table_name)))

    return result


def get_max_version(connection, schema_name, table_name):
    prefix = prefix_dict.get(connection.database_type)

    result = query(connection, template('%s_discover_columns' % prefix,
                                        dict(schema_name=schema_name,
                                             table_name=table_name)))

    if len(result) == 0:
        return 1

    tpl = template('%s_max_version' % prefix,
                     dict(schema_name=schema_name,
                          table_name=table_name),
                     connection.database_type)

    result = query(connection, tpl)

    if len(result) > 0 and result[0].version is not None:
        return result[0].version+1
    else:
        return 1


class InputType(object):
    
    def __init__(self, name, params=None):
        if params is None:
            params = {}
  
        self.name = name
        self.params = params

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


def extract_number(number_str, decimal=None):
    #TODO: handle decimal=False/True
    #TODO: regexp \s*[-+]?\s*[€$%]?[0-9,\.]\+
    separators = []   
    groups = ['']
    groups_index = 0
    sign = None

    for c in number_str:

        if c in ['.', ',']:
            separators.append(c)
            groups.append('')
            groups_index += 1

        elif c in [' ', '€', '$', '%']:
            continue

        elif c in ['+', '-'] and sign is None:
            sign = c

        elif c in '1234567890':
            groups[groups_index] += c
            
            if sign is None:
                sign = '+'
        else:
            return None

    if len(groups) == 0:
        return None

    elif len(groups[0]) == 0:

        if len(''.join(groups)) == 0:
            return None

        number = Decimal('%s0.%s' % (sign, ''.join(groups)))
        precision = len(number.as_tuple().digits)
        scale = - number.as_tuple().exponent
        input_type = InputType('numeric', dict(precision=precision, scale=scale))
        return number, input_type

    elif len(groups) == 1:
        return int(groups[0]), InputType('int')

    elif len(groups) > 1:
        number = Decimal('%s%s.%s' % (sign,''.join(groups[0:-1]), groups[-1]))
        precision = len(number.as_tuple().digits)
        scale = - number.as_tuple().exponent
        input_type = InputType('numeric', dict(precision=precision, scale=scale))
        return number, input_type

    else:
        return None


def detect_dayfirst(dates):

    first_test = None
    second_test = None

    try:
        for i in dates:
            dateutil_parse(i, dayfirst=True)

        first_test = True
    except:
        first_test = False

    try:
        for i in dates:
            dateutil_parse(i, dayfirst=False)

        second_test = True
    except:
        second_test = False
   
    if first_test != second_test:
        return first_test
    else:
         None


def parse_csv(input_data, delimiter=';', quotechar='"'):
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(input_data, delimiters=';,\t')
    except:
        csv.register_dialect('dlb_excel', delimiter=delimiter, quotechar=quotechar)
        dialect = csv.get_dialect('dlb_excel')

    data = input_data
    data = data.splitlines()
    data = csv.reader(data, dialect=dialect)
    return list(data)


def detect_type(value, current_type=None):

    if value is None:
        return None, current_type       

    if current_type is None:
        extract_result = extract_number(value)

        if extract_result is not None:
            return extract_result

        try:
            result = dateutil_parse(value)
            #TODO: handle timezones
            return result, InputType('timestamp', dict(dayfirst=None, last_value=result))

        except Exception as e:
            pass
                    
        if value.lower() in ['true', 't', 'yes', 'y']:
            return True, InputType('bool')

        elif value.lower() in ['false', 'f', 'no', 'n']:
            return False, InputType('bool')

        return value, InputType('text')

    elif current_type.name == 'text':
        return value, InputType('text')

    elif current_type.name == 'numeric':

        if value == '' or value is None:
            return None, current_type

        extract_result = extract_number(value)

        # not a number
        if extract_result is None:
            return value, InputType('text')

        # it is a number
        new_type = extract_result[1]
        value = extract_result[0]

        if new_type.name == 'numeric':
            # enlarge precision to hold new value if necessary
            
            precision = max(current_type.params['precision'], new_type.params['precision'])
            scale = max(current_type.params['scale'], new_type.params['scale'])

            # make sure that it will hold old value
            natural_lenght = max(precision - scale, current_type.params['precision'] - current_type.params['scale'])
            precision = max(precision, natural_lenght+scale)
        
        elif new_type.name == 'int':
            natural_lenght = len(str(value))
            precision = max(current_type.params['precision'], natural_lenght)
            scale = current_type.params['scale']
            precision = max(natural_lenght + scale, precision)
            
        return value, InputType('numeric', dict(precision=precision, scale=scale))

    elif current_type.name == 'int' or current_type.name == 'integer':
        if value == '' or value is None:
            return None, current_type

        else:
            extract_result = extract_number(value)

            # not a number
            if extract_result is None:
                return value, InputType('text')

            return extract_result
        
    elif current_type.name == 'bool':
        if value.lower() in ['true', 't', 'yes', 'y']:
            return True, InputType('bool')

        elif value.lower() in ['false', 'f', 'no', 'n']:
            return False, InputType('bool')

    elif current_type.name.startswith('timestamp'):
        #TODO: handle timezones
        result = re.findall('\d+|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec', value.lower())

        if len(result) < 3 or len(result) > 8:
            return value, InputType('text')

        try:
            dateutil_parse(value, dayfirst=current_type.params.get('dayfirst'))

        except:
            return value, InputType('text')
        
        if current_type.params.get('dayfirst') is None:
            #FIXME: dayfirst = detect_dayfirst(current_type.params['last_value'])
            dayfirst = detect_dayfirst(value)

        return value, InputType('timestamp', dict(dayfirst=dayfirst, last_value=value))

    else:
        return value, current_type


def preprocess_csv_data(input_data):
    data = parse_csv(input_data)

    if len(data) < 2:
        raise ValueError('Not enough data')

    header = data[0]
    formated_header = []
    collisions = {}
    generic_name_counter = 0

    for column in header:

        letters_and_digits = string.ascii_lowercase + string.digits
        buf = ''

        if len(column) > 0:
            for i, char in enumerate(column.lower()):

                if i == 0 and char in string.digits:
                    buf += '_'

                elif char not in letters_and_digits:
                    buf += '_'

                else:
                    buf += char

            # check for collisions
            if buf in formated_header:
                suffix = collisions.get(buf)

                if suffix is not None:
                    collisions[buf] = suffix+1 
                    buf = buf + '_' +  str(suffix+1)

                else:
                    buf += '_1'
        else:
            generic_name_counter += 1
            buf = 'column_%i' % generic_name_counter

        formated_header.append(buf)
    
    return formated_header, header, data[1:]


def get_row_data_types(header, row, current_types=None, detect_type_func=None):

    if detect_type_func is None:
        detect_type_func = detect_type

    if current_types is None:
        current_types = {}

    new_types = dict(current_types)

    result_row = []

    for i, value in enumerate(row):

        if i >= len(header):
            break

        v, new_types[header[i]] = detect_type_func(value, current_types.get(header[i]))
        result_row.append(v)

    return result_row, new_types


def get_data_types(header, data, current_types=None, detect_type_func=None):

    if detect_type_func is None:
        detect_type_func = detect_type

    if current_types is None:
        current_types = {}

    result_data = []
    for row in data:

        result_row = []
        for i, value in enumerate(row):
            v, current_types[header[i]] = detect_type_func(value, current_types.get(header[i]))
            result_row.append(v)
            
        result_data.append(result_row)

    return result_data, current_types


if __name__ == '__main__':
    print(detect_type('2017-05-17 12:27:25.294589'))
