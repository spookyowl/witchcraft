import itertools
import os.path

from witchcraft.utils import build_tuple_type
from witchcraft.template import Template

base_path = os.path.dirname(os.path.abspath(__file__))

__buildin_filter = filter
__buildin_map = map
__query_paths = ['./', './queries', './templates', os.path.join(base_path, 'queries')]
__template_cache = {}


def th(*operations):
    args = []
    for item in operations:
        args.extend(list(item)[1:])
        args = [item[0](*args)]

    return args[0]


def set_query_path(path):
    global __query_paths

    if isinstance(path, list):
        __query_paths = path

    elif isinstance(path, str) or isinstance(path, unicode):
        __query_paths = [path] 


def conv_symbol_name(s):
    s = s.lstrip(u'\ufdd0:')
    return s.replace('-','_')


def dict_merge(a, b):
    return dict(a.items() + b.items())
  

#TODO: think about how to make query lazy
def query(connection, sql_query):
    result_proxy = connection.execute(sql_query)
    result_type = build_tuple_type(*result_proxy.keys())
    result = map(lambda r: result_type(dict(r)), result_proxy)
    result_proxy.close()
    return result
 

def execute(connection, sql_query):
    result_proxy = connection.execute(sql_query)
    result_proxy.close()
    return result_proxy


def template(template_name, context = None, dialect = None):
    cache_key = template_name

    if context is not None:
        
        conv_context = {}
        for k,v in context.items():
            conv_context[conv_symbol_name(k)] = v
    else:
        conv_context = {}

    found = __template_cache.get(cache_key)

    if found is not None:
        #TODO: handle dialect when teplate is used
        return Template(found, dialect).substitute(**conv_context)

    if not template_name.endswith('.sql'):
        template_name += '.sql'

    template_name = template_name.lstrip(u'\ufdd0:')

    filename_patterns = [template_name,
                         template_name.replace('-', '_'),
                         template_name.replace('_', '-')]   

    path_pattern = itertools.product(__query_paths, filename_patterns)

    for t in path_pattern:
        file_path = os.path.join(t[0], t[1])

        if os.path.isfile(file_path):

            with open(file_path) as query_file:
                query_tpl = query_file.read()

            __template_cache[cache_key] = query_tpl
            #TODO: cache template instance
            return Template(query_tpl, dialect).substitute(**conv_context)

    raise ValueError('Template not found')


def filter(data, func):
    return __buildin_filter(func, data) 


def each(data, func):
    return __buildin_map(func, data)


def filter_by(data, key_name, value):
    
    def filter_fn(item):
        return item['key_name'] == value
    
    return __buildin_filter(filter_fn, data) 


def group_by(data, key_func, *operations):
    memo = {}

    for item in data:
        key = key_func(item)
        group_list = memo.get(key, [])
        group_list.append(item)
        memo[key] = group_list

    #print 'G', memo

    for key, value in memo.items():
        for op_fn in operations:

            if isinstance(op_fn, tuple):
                args = [value]
                args.extend(list(op_fn)[1:])
                value = op_fn[0](*args)

            else:
                value = op_fn(value)

        memo[key] = value

    return memo


def group_by_columns(data, key_names, *operations):
    operations = list(operations)

    def post_process(d):
        #if isinstance(d,list):
        return ommit_columns(d, key_names)

    operations.insert(0, post_process)
    return group_by(data, lambda d: select_columns(d, key_names), *operations)


def columns(data, columns):
    return to_tuple(data, columns)


#TODO: allow renaming of columns
def select_columns(data, columns):

    tuple_type = build_tuple_type(*columns)

    if isinstance(data, list):
        return map(tuple_type, data)
    else:
        return tuple_type(data)


def ommit_columns(tuple_set, columns):
    columns = set(columns)

    def ommit_fn(item):
        keys = item.keys()
        keys = list(set(item.keys()) - set(columns))
        result_type = build_tuple_type(*keys)
        return result_type(item.select(keys))
        
    return map(ommit_fn, tuple_set)


def aggregate(iterable, *aggregators):
    memo = iterable

    for agg_fn in aggregators:
        memo = agg_fn(iterable, memo)

    return memo


def aggregate_list_of(iterable, extract_key):
    #print 'L', iterable
    result = list(map(lambda i: i[extract_key], iterable))
    return result


def aggregate_first_row(iterable):
    return iterable[0]


def aggregate_first_of(iterable, extract_key):
    return iterable[0][extract_key]


def _flatten(keys, items, column_names):
    #if (len(column_names) - len(keys)) == 0:
    #    return items
    key_names = column_names[:len(keys)]
    result = []
    key_part = dict(zip(key_names, keys))
    if isinstance(items, dict):
        first_values = items.values()

        #print 'first_values', first_values
        #if len(first_values) and (isinstance(first_values, dict) or isinstance(first_values, OrderedDict)):
        #    items = flatten_all(items, column_names[len(keys):])
        #else:
        #if not (isinstance(first_values, tuple) or isinstance(first_values, list)):
        #    items = (items, )

        if len(column_names[len(keys):]):
            #print items, column_names
            items = flatten_dict(items, column_names[len(keys):])

        else:
            items = (items, )

    #print items
    for i in items:
        if not isinstance(i, dict):
            #print i
            i = i.asdict()
        result.append(dict(key_part, **i))

    return result
    

#TODO: really required???
def flatten_dict(data, column_names):
    result = []

    if isinstance(data, dict):
        #print data.items()
        for key, i in data.items():

            if len(i) == 0:
                continue

            key = key.values()
            #print key
            if not (isinstance(key, tuple) or isinstance(key, list)):
                key = (key,)

            result.extend(_flatten(key, i, column_names))

        return result

    else:
        return data


def flatten(mapping):

    keys = mapping.keys()
    values = mapping.values()

    def find_non_empty(v):
        for i in v:
            if len(i) > 0:
                return i

    if len(keys) == 0:
        return []

    value = find_non_empty(values)

    if isinstance(value, list):
        list_value = True
        value_keys = value[0].keys()

    else:
        list_value = False
        value_keys = value.keys()

    keys = keys[0].keys() + value_keys

    ResultType = build_tuple_type(*keys)

    result = []

    if list_value:
        for key, value in mapping.items():
            for item in value:
              result.append(ResultType(key.items() + item.items()))
                
    else:
        for key, value in mapping.items():
            result.append(ResultType(key.items() + value.items()))

    return result


# itemize(['name'] 'columns')
def itemize_dict(mapping, columns):
    result = []
    result_type = build_tuple_type(*columns)

    for key, value in mapping.items():
        item = {}

        for i, c in enumerate(columns[:-1]):
            item[c] = key.values()[i]

        item[columns[-1]] = value
        result.append(result_type(item))

    return result


def complement(left, right, func):
    left_keys = set(left.keys())
    right_keys = set(right.keys())

    intersection = left_keys - right_keys
    
    result = {}

    if combine_fn is None:
        for key in intersection:
                result[key] = left[key]
    
    else:
         for key in intersection:
                result[key] = combine_fn(left[key])
       
    return result


def intersect(left, right, combine_fn):
    left_keys = set(left.keys())

    if isinstance(right, dict):
        right_keys = set(right.keys())

    else:
        right_keys = right

    intersection = left_keys & right_keys
    
    result = {}

    if combine_fn is not None:

        if isinstance(right, dict):

            for key in intersection:
                result[key] = combine_fn(left[key], right[key], key)

        else:

            for key in intersection:
                result[key] = combine_fn(left[key], key)

    else:

        for key in intersection:
            result[key] = left[key]

    return result


def join_by_columns(left, right, left_keys, right_keys):

    mlk = set(left[0].keys()) - set(left_keys)
    mrk = set(right[0].keys()) - set(right_keys)

    tuple_type = build_tuple_type(list(mrk)+list(mlk))

    def merge(left, right, k):
        
        if len(right) == 0:
            return tuple_type(left)

        result = []

        for li in left:
            
            for ri in right:
                r = tuple_type(dict(li.items() + ri.items()))
                result.append(r)
            
        return result

    left = group_by_columns(left, left_keys)
    right = group_by_columns(right, right_keys)

    result = intersect(left, right, merge)
    return flatten(result)


#TODO: join_by_fn
def union():
    pass


def to_tuple(data, keys=None):
    if keys is None:
        keys = data[0].keys()
    tuple_type = build_tuple_type(*keys)
    return map(tuple_type, data)
