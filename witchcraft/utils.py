import string
from pprint import pformat
from collections import OrderedDict
from datetime import datetime, date
from decimal import Decimal
import itertools
import os
base_path = os.path.dirname(os.path.abspath(__file__))

try:
    from UserDict import DictMixin
except ImportError:
    from collections import MutableMapping as DictMixin

try:
    text = unicode
except NameError:
    text = str 


# Use the same json implementation as itsdangerous because Flask does that
# and we depend on it
try:                                                                               
    from itsdangerous import simplejson as _json                                
except ImportError:                                                             
    from itsdangerous import json as _json                                      


def convert_column_name(column_name):
    #TODO: only A-z0-9_ allowd sanitize with regexp, throw exception
    column_name = column_name.lower()
    column_name = column_name.replace(' ','_')
    return column_name.replace('-','_')

def to_camel_case(snake_str):
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])

def to_kebab_case(column_name):
    return column_name.replace('_','-')


class ColumnNameGenerator(object):

    def __init__(self):
        self.header_memory = []
        self.collisions = {}
        self.generic_name_counter = 0    
        self.letters_and_digits = string.ascii_lowercase + string.digits

    def next_name(self, orignal_name):

        buf = ''

        if len(orignal_name) > 0:

            for i, char in enumerate(orignal_name.lower()):

                if i == 0 and char in string.digits:
                    buf += '_'

                elif char not in self.letters_and_digits:
                    buf += '_'

                else:
                    buf += char

            # check for collisions
            if buf in self.header_memory:
                suffix = self.collisions.get(buf)

                if suffix is not None:
                    self.collisions[buf] = suffix+1 
                    buf = buf + '_' +  str(suffix+1)

                else:
                    buf += '_1'

        else:
            self.generic_name_counter += 1
            buf = 'column_%i' % self.generic_name_counter

        self.header_memory.append(buf)

        return buf


def coalesce(*values):
    for v in values:
        if v is not None:
            return v  


def chainlist(iter_of_lists):
    return list(itertools.chain(*iter_of_lists))


class TupleMeta(object):

    def __init__(self, input_data=None, select_vector=None, **kw_args):
        buf = {}

        if input_data is None:
            input_data = kw_args

        elif isinstance(input_data, list):
            input_data = dict(input_data)

        elif not (isinstance(input_data, dict) or isinstance(input_data, TupleMeta)):
            raise ValueError('input must be either of type dict or list')

        if select_vector is not None and len(select_vector) != 0:

            if len(select_vector) > len(self.__slots__):
                raise ValueError('Source select vector is too long')
        else:
            select_vector = self.__slots__

        for k,v in input_data.items():
            buf[convert_column_name(k)] = v 
        
        for i, slot_key in enumerate(self.__slots__):

            if i < len(select_vector):
                setattr(self, slot_key, buf.get(select_vector[i]))
            else:
                setattr(self, slot_key, buf.get(slot_key))

    def __getitem__(self, k):
        if isinstance(k, int):
            return getattr(self,self.__slots__[k])

        if k in self.__slots__:
            return getattr(self, k)

    def __contains__(self, value):
        return value in self.__slots__

    def __eq__(self, value):
        if len(self.__slots__) == 1:
            stored_value = self[0]

            if isinstance(value, type(stored_value)) \
                or (isinstance(stored_value, text) and isinstance(value, str)) \
                or (isinstance(stored_value, str) and isinstance(value, text)):

                return stored_value == value
       
        for i, k in enumerate(self.__slots__):
            if getattr(self, k) != value[i]:
                return False

        return True

    def __ne__(self, value):

        for k in self.__slots__:
            if getattr(self, k) != value[k]:
                return True

        return False

    def __repr__(self):
        d = self.items()
        return 'Tuple %s' % d.__repr__()

    def __hash__(self):

        def hash_value(v):
            
            if isinstance(v, dict):
                return frozenset(v.items())

            return v

        if len(self.__slots__) == 1:
            return hash(hash_value(self.values()[0]))

        return hash(tuple([hash_value(v) for v in self.values()]))

    def __len__(self):
        return len(self.__slots__)

    def __iter__(self):
       return (getattr(self,k) for k in self.__slots__)

    def get(self, k, default=None):
        if k in self.__slots__:

            try:
                return getattr(self, k)

            except AttributeError:
                return default

        return default

    def keys(self):
        return self.__slots__

    def items(self):
        result = []
        for k in self.__slots__:
            result.append((k, getattr(self, k)))

        return result

    def values(self):
        result = []

        for k in self.__slots__:

            if hasattr(self,k):
                result.append(getattr(self, k))

            else:
                result.append(getattr(self, None))

        return result

    def select(self, keys):
        result = {}

        for k in keys:
            if k in self.__slots__:
                result[k] = getattr(self, k)

        return result

    def __dict__(self):
        return self.asdict()

    def asdict(self):
        result = {}

        for k in self.__slots__:
            result[k] = getattr(self, k) 

        return result

    def asjson(self):
        return _json.dumps(self.asdict(), cls=TupleJSONEncoder)


def build_tuple_type(*columns):

    if len(columns) == 1 and isinstance(columns[0], list):
        columns = columns[0]

    columns = [convert_column_name(c) for c in columns]

    class Tuple(TupleMeta): 
        __slots__ = columns

    return Tuple


class TupleJSONEncoder(_json.JSONEncoder):
    
    def default(self, obj):

        if isinstance(obj, datetime):
            return obj.isoformat()

        if isinstance(obj, date):
            return obj.isoformat()

        if isinstance(obj, Decimal):
            return str(obj)

        asdict_op = getattr(obj, "asdict", None)
        if callable(asdict_op):
            return obj.asdict()

        else:
            return _json.JSONEncoder.default(self, obj)


class BaseItem(object):
    """Base class for all scraped items."""
    pass


class Field(dict):
    """Container of field metadata"""
    pass


class ItemMeta(type):

    def __new__(mcs, class_name, bases, attrs):
        fields = OrderedDict()
        new_attrs = OrderedDict()

        for n, v in attrs.iteritems():
            if isinstance(v, Field):
                fields[n] = v
            else:
                new_attrs[n] = v

        cls = super(ItemMeta, mcs).__new__(mcs, class_name, bases, new_attrs)
        cls.fields = cls.fields.copy()
        cls.fields.update(fields)
        return cls


class DictItem(DictMixin, BaseItem):

    fields = OrderedDict()

    def __init__(self, *args, **kwargs):
        self._values = OrderedDict()
        if args or kwargs:  # avoid creating dict for most common case
            for k, v in dict(*args, **kwargs).iteritems():
                self[k] = v

    def __getitem__(self, key):
        if key in self.fields:
            return self._values.get(key)

        else:
            raise KeyError("%s does not support field: %s" %
                (self.__class__.__name__, key))

    def __setitem__(self, key, value):
        if key in self.fields:
            self._values[key] = value
        else:
            raise KeyError("%s does not support field: %s" %
                (self.__class__.__name__, key))

    def __delitem__(self, key):
        del self._values[key]

    def __getattr__(self, name):
        if name in self.fields:
            raise AttributeError("Use item[%r] to get field value" % name)
        raise AttributeError(name)

    def __setattr__(self, name, value):
        if not name.startswith('_'):
            raise AttributeError("Use item[%r] = %r to set field value" %
                (name, value))
        super(DictItem, self).__setattr__(name, value)

    def keys(self):
        return self._values.keys()

    def __repr__(self):
        return pformat(dict(self))

    def copy(self):
        return self.__class__(self)

    def __len__(self):
        return len(self.fields)

    def __iter__(self):
        return iter(self.fields.keys())
    
    def is_empty(self):
        return len(self._values) == 0

    def load(self, source):
        getter = None

        def get_item_value(c, keys):

            for k in keys:
                v = source.get(k)

                if v is not None:
                    return v

            return None

        if isinstance(source, list):
            def faa(c,k):
                if c >= len(source):
                    return None

                return source[c]

            getter = faa
            
            #getter = lambda c,k: source[c]

        elif isinstance(source, dict):
            getter = get_item_value

        else:
            asdict_op = getattr(source, "asdict", None)
            if callable(asdict_op):
                source = source.asdict()
                getter = get_item_value
            else:
                raise ValueError('Input not recognized. It must be list,dict or have asdict method')

        for c, key in enumerate(self.fields.keys()):
            alt_keys = self.fields[key].get('source_names', [])

            if self.fields[key].get('from_camel_case', False):
                alt_keys = [to_camel_case(key)]

            if self.fields[key].get('from_kebab_case', False):
                alt_keys = [to_kebab_case(key)]
                
            value = getter(c, [key] + alt_keys)

            if value is None:
                self[key] = None

            elif (isinstance(value, str) or isinstance(value, text)) and value == '' and self.fields[key].get('psql_type') in ('text', 'bytea'):
                self[key] = None

            elif (isinstance(value, str) or isinstance(value, text)) and value == '' and self.fields[key].get('psql_type') in ('int', 'bigint', 'numeric'):
                self[key] = None
            
            elif isinstance(value, str):
                self[key] = value

            elif isinstance(value, text):
                self[key] = value.encode('utf-8', 'ignore')
            
            elif isinstance(value, list):
                self[key] = _json.dumps(value, cls=TupleJSONEncoder)

            elif isinstance(value, dict):
                self[key] = _json.dumps(value, cls=TupleJSONEncoder)
            
            else:
                self[key] = value


    def select(self, keys):
        result = []

        for key in keys:
            if key in self.keys():
                result.append(self[key])

        return result

    def add_field(self, name, **kwargs):
        field = Field(**kwargs)
        self.fields[name] = field


class Item(DictItem):

    __metaclass__ = ItemMeta


def read_batch(iterator, batch_size=None):
    
    if batch_size is None:
        batch_size = 10000

    result = []

    for i in range(batch_size):
        
        try:
            result.append(next(iterator))

        except StopIteration:
            return result

    return result 


def remove_null_rows(batch, primary_key):

    def ff(item):
        for k in primary_key:
            if item[k] is None:
                return False

        empty_count = 0

        for c in item.values():
            if c is None:
                empty_count +=1

        if empty_count == len(item):
            return False

        #print (item, len(item), empty_count, item.is_empty())
        return True

    return list(filter(ff,batch))


class seekable(object):
    
    def __init__(self, iterable):
        self.pos = 0
        self.buf = []
        self.buf_iterator = iter(self.buf)
        self.disabled = False

    def __iter__(self):
        return self

    def __next__(self):
        self.disabled = True

        if self.pos < len(self.buf):
            b=self.buf[self.pos]
            self.pos+=1
            return b
        else:
            return next(self.iterator)

    def __getitem__(self, index):

        # getting an item is still posible but we are rather raising error
        # to prevent incorrect usage
        if not self.disabled:

            if index < len(self.buf):
                return self.buf[index]
            else:
                self.seek(index)
                return self.buf[index]

        else:
            raise ValueError('Seeking disabled iteration already started')

    def seek(self, index):

        if not self.disabled:

            for i in range(index-(len(self.buf)-1)):
                self.buf.append(next(self.iterator))
                    
        else:
            raise ValueError('Seeking disabled iteration already started')

    def is_empty(self):
        try:
            self.seek(1)
            return False

        except StopIteration:
            return True
     
    def has_one_element(self):
        test_first = False
        test_second = False

        try:
            self.seek(1)
            test_first = True

        except StopIteration:
            test_first = False

        try:
            self.seek(2)
            test_first = False

        except StopIteration:
            test_first = True

        return test_first and not test_second

    def get_buffer_iterator(self):
        return iter(self.buf)


__query_paths = ['./', './queries', './templates', os.path.join(base_path, 'queries')]

def find_query_template(template_name):

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

            return query_tpl

