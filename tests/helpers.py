import random
import string
import time
import io
from functools import wraps
from random import randint
from urllib.parse import urlparse

from tests.settings import CONTAINER_PREFIX


def random_container_name():
    rand_chars = ''.join(random.sample(string.ascii_letters, 8)).lower()
    return '%s-%s' % (CONTAINER_PREFIX, rand_chars)


def uri_validator(uri):
    if not uri:
        return False

    try:
        result = urlparse(uri)
        return True if [result.scheme, result.netloc, result.path] else False
    except TypeError:
        return False


def rate_limited(delay: int = 1):
    """Rate-limits the decorated function."""

    def decorate(func):
        @wraps(func)
        def rate_limited_function(*args, **kwargs):
            time.sleep(delay)
            return func(*args, **kwargs)

        return rate_limited_function

    return decorate

def binary_iostreams(count=10):
    files = {}
    for i in range(count):
        data = b'\x01'*1024*randint(1,10)
        files[i] = io.BytesIO(data)
    return files



