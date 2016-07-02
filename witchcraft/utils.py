
def build_tuple_type(*columns):

    class Tuple(object):
        __slots__ = columns

        def __init__(self, d=None, **kw):
            if d is None:
                d = kw

            for k in self.__slots__:
                setattr(self, k, d.get(k))
    
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
           
            for i,k in enumerate(self.__slots__):
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
            return 'WCF ' + d.__repr__()

        def __hash__(self):

            if len(self.__slots__) == 1:
                return hash(self.values()[0])

            return hash(tuple(self.values()))

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

        def asdict(self):
            result = {}

            for k in self.__slots__:
                result[k] = getattr(self, k) 

            return result

    return Tuple
