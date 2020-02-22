import random
import string
import time
import typing
import io
import asyncio
import tempfile
import contextvars
import functools
from functools import wraps
from random import randint
from urllib.parse import urlparse

from tests.settings import CONTAINER_PREFIX

T = typing.TypeVar("T")
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

async def run_in_threadpool(
    func: typing.Callable[..., T], *args: typing.Any, **kwargs: typing.Any
) -> T:
    loop = asyncio.get_event_loop()
    if contextvars is not None:  # pragma: no cover
        # Ensure we run in the same context
        child = functools.partial(func, *args, **kwargs)
        context = contextvars.copy_context()
        func = context.run
        args = (child,)
    elif kwargs:  # pragma: no cover
        # loop.run_in_executor doesn't accept 'kwargs', so bind them in here
        func = functools.partial(func, **kwargs)
    return await loop.run_in_executor(None, func, *args)

class UploadFile:
    """
    An uploaded file included as part of the request data.
    """

    spool_max_size = 1024 * 1024

    def __init__(
        self, filename: str, file: typing.IO = None, content_type: str = ""
    ) -> None:
        self.filename = filename
        self.content_type = content_type
        if file is None:
            file = tempfile.SpooledTemporaryFile(max_size=self.spool_max_size)
        self.file = file

    async def write(self, data: typing.Union[bytes, str]) -> None:
        await run_in_threadpool(self.file.write, data)

    async def read(self, size: int = None) -> typing.Union[bytes, str]:
        return await run_in_threadpool(self.file.read, size)

    async def seek(self, offset: int) -> None:
        await run_in_threadpool(self.file.seek, offset)

    async def close(self) -> None:
        await run_in_threadpool(self.file.close)
