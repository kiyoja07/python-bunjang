import functools


class cached_property(object):
    def __init__(self, fget):
        self.fget = fget

        functools.update_wrapper(self, fget)

    def __get__(self, obj, cls):
        if obj is None:
            return self

        value = self.fget(obj)
        setattr(obj, self.fget.__name__, value)
        return value
