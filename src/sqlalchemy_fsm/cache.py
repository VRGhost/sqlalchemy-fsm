"""Caching tools/classes"""

import weakref


class DictCache(object):
    """Generic object that uses dict-like object for caching."""

    __slots__ = ("cache", "getDefault")

    def __init__(self, dictObject, getDefault):
        self.cache = dictObject
        self.getDefault = getDefault

    def getValue(self, key):
        """A method is faster than __getitem__"""
        try:
            return self.cache[key]
        except KeyError:
            out = self.getDefault(key)
            self.cache[key] = out
            return out


def weakValueCache(getFunc):
    """A decorator that makes a new DictCache using function provided as value getter"""
    return DictCache(weakref.WeakValueDictionary(), getFunc)


def dictCache(getFunc):
    """Generic dict cache decorator"""
    return DictCache({}, getFunc)


class caching_attr(object):
    """An attribute that is only computed once."""

    __slots__ = ("getValueFn", "value")

    def __init__(self, getValueFn):
        self.getValueFn = getValueFn

    def __get__(self, instance, owner):
        try:
            return self.value
        except AttributeError:
            # Not cached yet
            pass
        if instance:
            out = self.getValueFn(instance)
            self.value = out
        else:
            raise NotImplementedError("Only works on instances at the moment")
        return out

    def __delete__(self, instance):
        del self.value
