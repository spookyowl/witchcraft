from pprint import pformat
from UserDict import DictMixin
from collections import OrderedDict

from json import JSONEncoder
import json

def convert_column_name(column_name):
    return column_name.lower().replace(' ','_')


class TupleMeta(object):

    def __init__(self, d=None, **kw):
        buf = {}
        
        if d is None:
            d = kw

        elif isinstance(d, list):
            d = dict(d)


        for k,v in d.items():
            buf[convert_column_name(k)] = v 
        
        for k in self.__slots__:
            setattr(self, k, buf.get(k))

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
                or (isinstance(stored_value, unicode) and isinstance(value, str)) \
                or (isinstance(stored_value, str) and isinstance(value, unicode)):

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
        d = self.asdict()
        return 'Tuple %s' % d.__repr__()

    def __hash__(self):

        if len(self.__slots__) == 1:
            return hash(self.values()[0])

        return hash(tuple(self.values()))

    def __len__(self):
        return len(self.__slots__)

    def get(self, k, default=None):
        if k in self.__slots__:

            try:
                return getattr(self, k)

            except AttributeError:
                return default

        return None

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
            result.append(getattr(self, k))

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
        return json.dumps(self.asdict(), cls=TupleJSONEncoder)


def build_tuple_type(*columns):

    if len(columns) == 1 and isinstance(columns[0], list):
        columns = columns[0]

    columns = [convert_column_name(c) for c in columns]

    class Tuple(TupleMeta): 
        __slots__ = columns

    return Tuple


class TupleJSONEncoder(JSONEncoder):
    
    def default(self, obj):

        if isinstance(obj, TupleMeta):
            return obj.asdict()

        else:
            return JSONEncoder.default(self, obj)





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
        return self._values[key]

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

    def load(self, source):
        
        for key in self.fields.keys():

            value = source.get(key)

            if value is None:
                self[key] = None

            elif isinstance(value, unicode):
                self[key] = value.encode('utf-8', 'ignore')
            
            elif isinstance(value, list):
                self[key] = json.dumps(value)

            elif isinstance(value, dict):
                self[key] = json.dumps(value)

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
