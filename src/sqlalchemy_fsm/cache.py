"""Caching tools/classes"""

import weakref


class DictCache(object):
    """Generic object that uses dict-like object for caching."""

    __slots__ = ("cache", "get_default")

    def __init__(self, dict_object, get_default):
        self.cache = dict_object
        self.get_default = get_default

    def get_value(self, key):
        """A method is faster than __getitem__"""
        try:
            return self.cache[key]
        except KeyError:
            out = self.get_default(key)
            self.cache[key] = out
            return out


def weak_value_cache(get_func):
    """A decorator that makes a new dict_cache using function provided as value getter"""
    return DictCache(weakref.WeakValueDictionary(), get_func)


def dict_cache(get_func):
    """Generic dict cache decorator"""
    return DictCache({}, get_func)
