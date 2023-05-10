"""
	Wrapped allows one to override/extend classes and objects even if 
	  there are weird constraints (like metaclasses).
"""

__copyright__ = """Copyright (C) 2020 Corso Systems"""
__license__ = 'Apache 2.0'
__maintainer__ = 'Andrew Geiger'
__email__ = 'andrew.geiger@corsosystems.com'


class Wrapped(object):
    """Faking inheritance using guide rails.

    Some classes are complex and have sophisticated metaclasses. 
    This class allows for wrapping such a thing transparently.
    For all intents and purposes, it will look and act like the wrapped type,
      but will not interfere with the metaclass's operations.

    IMPORTANT: Any subclass of this _must_ set the `_type` attribute!
      This is the class that would otherwise have been inherited from. 
    """
    
    __slots__ = ('_self',)

    _wrap_type = None
    _allow_identity_init = False
    
    def __init__(self, *args, **kwargs):
        """There's a lot of metamagic in the autogenerated. 
        We don't want to interfere with that, so we're wrapping the object instead.
        """
        if (    self._allow_identity_init 
            and len(args) == 1 
            and isinstance(args[0], self._wrap_type)):
            self._self = args[0]
        else:
            self._self = self._wrap_type(*args, **kwargs)

    def __getattr__(self, attribute):
        """Get from this class first, otherwise use the wrapped item."""
        try:
            return super(Wrapped, self).__getattr__(attribute)
        except AttributeError:
            return getattr(self._self, attribute)

    def __setattr__(self, attribute, value):
        """Set to this class first, otherwise use the wrapped item."""
        try:
            return super(Wrapped, self).__setattr__(attribute, value)
        except AttributeError:
            return setattr(self._self, attribute, value)