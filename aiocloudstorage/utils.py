"""Utility methods for Cloud Storage."""
import functools
import re
import io

_SENTINEL = object()

    

def rgetattr(obj, attr, default=_SENTINEL):
    """Get a nested named attribute from an object.

    Example: ::

        b = type('B', (), {'c': True})()
        a = type('A', (), {'b': b})()
        # True

    Source:
    `getattr-and-setattr-on-nested-objects <https://stackoverflow.com/questions/
    31174295/getattr-and-setattr-on-nested-objects/31174427>`__

    :param obj: Object.
    :type obj: object

    :param attr: Dot notation attribute name.
    :type attr: str

    :param default: (optional) Sentinel value, defaults to :class:`object()`.
    :type default: object

    :return: Attribute value.
    :rtype:  object
    """
    if default is _SENTINEL:
        _getattr = getattr
    else:
        def _getattr(obj_, name):
            return getattr(obj_, name, default)
    return functools.reduce(_getattr, [obj] + attr.split('.'))


def rsetattr(obj, attr, val):
    """Sets the nested named attribute on the given object to the specified
    value.

    Example: ::

        b = type('B', (), {'c': True})()
        a = type('A', (), {'b': b})()
        rsetattr(a, 'b.c', False)
        # False

    Source: `getattr-and-setattr-on-nested-objects <https://stackoverflow.com/
    questions/31174295/getattr-and-setattr-on-nested-objects/31174427>`__

    :param obj: Object.
    :type obj: object

    :param attr: Dot notation attribute name.
    :type attr: str

    :param val: Value to set.
    :type val: object

    :return: NoneType
    :rtype: None
    """
    pre, _, post = attr.rpartition('.')
    return setattr(rgetattr(obj, pre) if pre else obj, post, val)

def camelize(string, uppercase_first_letter=True):
    """
    Convert strings to CamelCase.

    Examples::

        >>> camelize("device_type")
        "DeviceType"
        >>> camelize("device_type", False)
        "deviceType"

    :func:`camelize` can be thought of as a inverse of :func:`underscore`,
    although there are some cases where that does not hold::

        >>> camelize(underscore("IOError"))
        "IoError"

    :param uppercase_first_letter: if set to `True` :func:`camelize` converts
        strings to UpperCamelCase. If set to `False` :func:`camelize` produces
        lowerCamelCase. Defaults to `True`.
    """
    if uppercase_first_letter:
        return re.sub(r"(?:^|_)(.)", lambda m: m.group(1).upper(), string)
    else:
        return string[0].lower() + camelize(string)[1:]

def underscore(word):
    """
    Make an underscored, lowercase form from the expression in the string.

    Example::

        >>> underscore("DeviceType")
        "device_type"

    As a rule of thumb you can think of :func:`underscore` as the inverse of
    :func:`camelize`, though there are cases where that does not hold::

        >>> camelize(underscore("IOError"))
        "IoError"

    """
    word = re.sub(r"([A-Z]+)([A-Z][a-z])", r'\1_\2', word)
    word = re.sub(r"([a-z\d])([A-Z])", r'\1_\2', word)
    word = word.replace("-", "_")
    return word.lower()
