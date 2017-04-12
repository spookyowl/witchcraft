import string
from decimal import Decimal
import csv

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
        discovered_pkeys = list(map(lambda r: r.column_name.lower(), discovered_pkeys))

        if len(primary_keys) != 0:

            if set(primary_keys) != set(discovered_pkeys):
                raise ValueError('Primary keys in destination table are not matching')

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

    #TODO: use SqlSession insert directly
    execute(connection, template('%s_insert' % prefix,
                            dict(schema_name=schema_name,
                                 table_name=table_name,
                                 column_names=column_names,
                                 columns=data_points[0].fields.items(),
                                 data_points=data_points),
                            connection.database_type))


def delete_data(connection, schema_name, table_name):
    prefix = prefix_dict.get(connection.database_type)

    #TODO: use SqlSession delete directly
    execute(connection, template('%s_delete' % prefix,
                            dict(schema_name=schema_name,
                                 table_name=table_name),
                            connection.database_type))


def discover_columns(connection):
    prefix = prefix_dict.get(connection.database_type)
    result = query(connection, template('%s_discover_columns' % prefix,
                                 dict(schema_name=schema_name,
                                      table_name=discovery_table_name)))


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

    if len(result) > 0:
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

    if len(groups) == 0 or len(groups[0]) == 0:
        return None

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


def parse_csv(input_data):
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(input_data)
    except:
        csv.register_dialect('dlb_excel', delimiter=';', quotechar='"')
        dialect = csv.get_dialect('dlb_excel')

    data = input_data
    data = data.splitlines()
    data = csv.reader(data, dialect=dialect)
    return list(data)


def detect_type(value, current_type=None):

    if current_type is None:
        extract_result = extract_number(value)

        if extract_result is not None:
            return extract_result

        try:
            result = dateutil_parse(value)
            return result, InputType('timestamp', dayfirst=None, last_value=result)

        except:
            pass
                    
        if value.lower() in ['true', 't', 'yes', 'y']:
            return True, InputType('bool')

        elif value.lower() in ['false', 'f', 'no', 'n']:
            return False, InputType('bool')

        return value, InputType('text')

    elif current_type.name == 'text':
        return value, InputType('text')

    elif current_type.name == 'numeric':
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

    elif current_type.name == 'int':
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

    elif current_type.name == 'timestamp':
        result = re.findall('\d+|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec', value.lower())

        if len(result) < 3 or len(result) > 7:
            return value, InputType('text')

        try:
            dateutil_parse(value, dayfirst=current_type.params.get('dayfirst'))

        except:
            return value, InputType('text')
        
        if current_type.params.get('dayfirst') is None:
            dayfirst = detect_dayfirst(current_type.params['last_value'])

        return value, InputType('timestamp', dayfirst=dayfirst, last_value=value)


def preprocess_csv_data(input_data):
    data = parse_csv(input_data)

    if len(data) < 2:
        raise ValueError('Not enough data')

    header = data[0]

    def format_header_column(column):
        letters_and_digits = string.ascii_lowercase + string.digits
        buf = ''

        for char in column.lower():

            if char not in letters_and_digits:
                buf += '_'

            else:
                buf += char

        return buf

    formated_header = list(map(format_header_column, header))
    
    return formated_header, header, data[1:]


def get_data_types(header, data, current_types=None):

    if current_types is None:
        current_types = {}

    result_data = []
    for row in data:

        result_row = []
        for i, value in enumerate(row):
            v, current_types[header[i]] = detect_type(value, current_types.get(header[i]))

            result_row.append(v)
            
        result_data.append(result_row)

    return result_data, current_types
