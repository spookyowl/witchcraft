
def build_tuple_type(*columns):

    class Tuple(object):
        __slots__ = columns

        def __init__(self, d=None, **kw):
            if d is None:
                d = kw

            for k in self.__slots__:
                setattr(self, k, d.get(k))
    
        def __getitem__(self, k):
            if k in self.__slots__:
                return getattr(self, k)
    
        def __contains__(self, value):
            return value in self.__slots__
    
        def __eq__(self, value):

            for k in self.__slots__:
                if getattr(self, k) != value[k]:
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
            return hash(tuple(self.values()))

        def get(self, k, default=None):
            if k in self.__slots__:

                try:
                    return getattr(self, k)

                except AttributeError:
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
