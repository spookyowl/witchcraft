import itertools
import os.path
from operator import itemgetter
from threading import Thread
from threading import Semaphore
from itertools import islice, zip_longest
import traceback

try:
    from queue import Queue
except ImportError:
    from Queue import Queue


from witchcraft.utils import build_tuple_type, __query_paths
from witchcraft.utils import coalesce, chainlist
from witchcraft.template import template

__buildin_filter = filter
__buildin_map = map


base_path = os.path.dirname(os.path.abspath(__file__))


def th(*operations):
    args = []
    for item in operations:
        args.extend(list(item)[1:])
        args = [item[0](*args)]

    return args[0]


def set_query_path(path):
    global __query_paths

    if isinstance(path, list):
        __query_paths = path.extend(__query_paths)

    elif isinstance(path, str) or isinstance(path, unicode):
        __query_paths.insert(0, path)

    __query_paths.append(os.path.join(base_path, 'queries'))


def dict_merge(a, b):
    c = b.copy()
    c.update(a)
    return c
  

#TODO: think about how to make query lazy
def query(connection, sql_query):
    if connection.connection is not None and callable(getattr(connection.connection, 'execute', None)):
        result_proxy = connection.connection.execute(sql_query)
    else:
        result_proxy = connection.execute(sql_query)

    result_type = build_tuple_type(*result_proxy.keys())
    result = list(map(lambda r: result_type(dict(r)), result_proxy))
    result_proxy.close()
    return result
 

def batch_fetch(connection, sql_query, batch_size):
    if connection.connection is not None and callable(getattr(connection.connection, 'execute', None)):
        
        result_proxy = connection.connection.execute(sql_query)
    else:
        result_proxy = connection.execute(sql_query)

    if result_proxy.returns_rows:
        result_type = build_tuple_type(*result_proxy.keys())
        #result = map(lambda r: result_type(dict(r)), result_proxy)

        while True:
            #chunk = list(islice(result, batch_size))
            chunk = islice(result_proxy, batch_size)
            chunk = list(map(lambda r: result_type(dict(r)), chunk))

            if len(chunk) != 0:
                yield chunk
            else:
                break

    result_proxy.close()


def batch_fetch_stream(connection, sql_query, batch_size):

    if connection.connection is not None and callable(getattr(connection.connection, 'execute', None)):
        result_proxy = connection.connection.execution_options(stream_results=True, yield_per=batch_size).execute(sql_query)

    else:
        result_proxy = connection.execution_options(stream_results=True, yield_per=batch_size).execute(sql_query)

    if result_proxy.returns_rows:
        result_type = build_tuple_type(*result_proxy.keys())

        while True:
            chunk = islice(result_proxy, batch_size)
            chunk = list(map(lambda r: result_type(dict(r)), chunk))

            if len(chunk) != 0:
                yield chunk
            else:
                break

    result_proxy.close()


def execute(connection, sql_query):
    if connection.connection is not None and callable(getattr(connection.connection, 'execute', None)):
        result_proxy = connection.connection.execute(sql_query)
    else:
        result_proxy = connection.execute(sql_query)

    row_count = result_proxy.rowcount
    result_proxy.close()
    return row_count


def filter(data, func):
    return __buildin_filter(func, data) 


def each(data, func):
    return list(__buildin_map(func, data))


def filter_by(data, key_name, value):
    
    def filter_fn(item):
        return item[key_name] == value
    
    return __buildin_filter(filter_fn, data) 


def filter_in(data, key_name, values):

    def filter_fn(item):
        return item[key_name] in values

    return __buildin_filter(filter_fn, data)


def group_by(data, key_func, *operations):
    memo = {}

    for item in data:
        key = key_func(item)
        group_list = memo.get(key, [])
        group_list.append(item)
        memo[key] = group_list

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
        return ommit_columns(d, key_names)

    operations.insert(0, post_process)
    return group_by(data, lambda d: select_columns(d, key_names), *operations)


def columns(data, columns):
    return to_tuple(data, columns)


def select_columns(data, columns):
    dest_column_names = []
    src_column_names = []    

    for c in columns:
        
        if isinstance(c, list) or isinstance(c, tuple):
            src_column_names.append(c[0])
            dest_column_names.append(c[1])
        else:
            src_column_names.append(c)
            dest_column_names.append(c)

    tuple_type = build_tuple_type(*dest_column_names)

    if isinstance(data, list):
        return list(map(lambda i: tuple_type(i, src_column_names), data))
    else:
        return tuple_type(data, src_column_names)


def ommit_columns(tuple_set, columns):
    columns = set(columns)

    def ommit_fn(item):
        keys = item.keys()
        keys = list(set(item.keys()) - set(columns))
        result_type = build_tuple_type(*keys)

        if isinstance(item, dict):
            return result_type({k:item[k] for k in item if k in keys})
        else:
            return result_type(item.select(keys))
        
    return list(map(ommit_fn, tuple_set))


def add_column_with(tuple_set, column_name, func):

    def add_fn(item):

        result_type = build_tuple_type(*(list(item.keys()) + [column_name]))

        if isinstance(item, dict):
            r = dict(**item)
        else:
            r = item.asdict()

        r[column_name] = func(r)
        return result_type(r)

    return list(map(add_fn, tuple_set))


def add_column(tuple_set, column_name, value):
    return add_column_with(tuple_set, column_name, lambda r: value)


def set_column_with(tuple_set, column_name, func):

    def set_fn(item):
        result_type = build_tuple_type(*list(item.keys()))

        if isinstance(item, dict):
            r = dict(**item)
        else:
            r = item.asdict()

        r[column_name] = func(r)
        return r

    return list(map(set_fn, tuple_set))


def set_column(tuple_set, column_name, value):
    return set_column_with(tuple_set, column_name, lambda r: value)


def aggregate(iterable, *aggregators):
    memo = iterable

    for agg_fn in aggregators:
        memo = agg_fn(iterable, memo)

    return memo


def aggregate_list_of(iterable, extract_key):
    result = list(map(lambda i: i[extract_key], iterable))
    return result

def aggregate_lists_of(iterable, extract_key):
    result = list(map(lambda i: i[extract_key], iterable))
    return list(itertools.chain(*result))

def aggregate_first_row(iterable):
    return iterable[0]


def aggregate_first_of(iterable, extract_key):
    return list(iterable)[0][extract_key]


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
        #print('first_values',first_values)
        #if not (isinstance(first_values, tuple) or isinstance(first_values, list)):
        #    items = (items, )

        if len(column_names[len(keys):]):
            #print items, column_names
            items = flatten_dict(items, column_names[len(keys):])

        else:
            items = (items, )

    if not (isinstance(items, tuple) or isinstance(items, list)):
        items = (items, )
    
    for i in items:

        # can be converted to dictionary
        asdict_op = getattr(i, "asdict", None)
        if callable(asdict_op):
            i = i.asdict()
        
        # is dictionary
        elif isinstance(i, dict):
            pass

        #is value
        else:
            i = {'value': i}

        result.append(dict(key_part, **i))

    return result
    

#TODO: really required??? - No, itemize_dict is sufficient
def flatten_dict(data, column_names):
    result = []

    if isinstance(data, dict):
        #print data.items()
        for key, i in data.items():

            if len(i) == 0:
                continue

            #print key
            if not (isinstance(key, tuple) or isinstance(key, list)):
                key = (key,)
            else:
                key = key.values()

            result.extend(_flatten(key, i, column_names))

        return to_tuple(result, column_names)

    else:
        return to_tuple(data, column_names)


def flatten(mapping, default_keys=None):

    keys = list(mapping.keys())
    values = mapping.values()

    def find_non_empty(v):
        for i in v:
            if len(i) > 0:
                return i

    if len(keys) == 0:
        return []

    value = find_non_empty(values)
    
    if value is None:
        return []

    elif isinstance(value, list):
        list_value = True
        value_keys = list(value[0].keys())

    else:
        list_value = False
        value_keys = list(value.keys())

    keys = keys[0].keys() + value_keys

    if default_keys is None:
        ResultType = build_tuple_type(*keys)
    else:
        ResultType = build_tuple_type(*default_keys)

    result = []

    if list_value:

        for key, value in mapping.items():

            for item in value:
                result.append(ResultType(key.items() + list(item.items())))
                
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

        # key is not dictionary
        if not callable(getattr(key,'values', None)):
            key = {'value': key}

        for i, c in enumerate(columns[:-1]):
            item[c] = list(key.values())[i]

        item[columns[-1]] = value
        result.append(result_type(item))

    return result


def complement(left, right, combine_fn=None):
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


def intersect(left, right, combine_fn=None):
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

    if len(left) == 0:
        return []

    mlk = set(left[0].keys())
    mrk = set(right[0].keys()) - set(right_keys)

    tuple_type = build_tuple_type(list(mrk) + list(mlk))

    def merge(left, right, k):
        result = []

        if len(right) == 0:

            for li in left:
                r = tuple_type(li.items() + k.items())
                result.append(r)

        else:
            for li in left:
                
                for ri in right:
                    r = tuple_type(dict(ri.items() + li.items() + k.items()))
                    result.append(r)
            
        return result

    left = group_by_columns(left, left_keys)
    right = group_by_columns(right, right_keys)

    for k,lv in left.items():
        rv = right.get(k)

        if rv is not None:
           left[k] = merge(lv, rv, k)
 
    return flatten(left, list(mrk)+list(mlk))


#TODO: join_by_fn
def union(iterables):
    return itertools.chain.from_iterable(iterables)


def to_tuple(data, keys=None):
    if keys is None:
        keys = data[0].keys()
    tuple_type = build_tuple_type(*keys)
    return list(map(tuple_type, data))


def sort_by_columns(iterable, *columns):
    return sorted(iterable, key=itemgetter(*columns))


def in_list(search_list):
    return lambda v: v in search_list


def parallel_map(func, maxthreads, items):
    semaphore = Semaphore(0)

    def worker():

        while True:
            item = q.get()

            try:
                import os
                func(item)

            except:
                print(traceback.format_exc())
    
            q.task_done()
            semaphore.release()

    q = Queue(maxthreads)

    for i in range(maxthreads):
         t = Thread(target=worker)
         t.daemon = True
         t.start()

    for c,item in enumerate(iter(items)):

        if c > 10000000:
            print("Safety stop condition reached")
            break

        q.put(item)

    q.join()


def distinct(iterable, *columns):

    keys = set()    

    def ffn(item):
        key = set()

        for cn in columns:
            cv = item.get(cn, None)

            if cv is not None:
                key.add(cv)

        key = tuple(key)

        if key not in keys:
            keys.add(key)
            return True
        
        return False

    return __buildin_filter(ffn, iterable)


def apply_func(func, args, kwargs):
    return func(*args, **kwargs)


def equal(left, right):
    
    if isinstance(left, list) and isinstance(right, list):

        for l,r in zip_longest(left,right):
            if l != r:
                return False

        return True

    elif not isinstance(left, list) and not isinstance(right, list):
        return left == right

    else:
        raise ValueError("Both arguments must be eigher of type 'list' or ")

