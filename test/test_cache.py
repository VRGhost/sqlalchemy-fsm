import pytest

from sqlalchemy_fsm import cache


class AttrCls(object):
    def __init__(self, rv_val):
        self._rvVal = rv_val

    @cache.caching_attr
    def test(self):
        self._rvVal += 1
        return self._rvVal


class TestAttrs(object):
    def test_cls(self):
        with pytest.raises(NotImplementedError):
            AttrCls.test

    def test_obj(self):
        obj = AttrCls(0)
        with pytest.raises(AttributeError):
            del obj.test  # was not cached
        assert obj.test == 1
        assert obj.test == 1
        del obj.test
        assert obj.test == 2
