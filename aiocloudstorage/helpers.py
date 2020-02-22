"""Helper methods for Cloud Storage."""
import hashlib
import mimetypes
import os
import re
from _hashlib import HASH
from typing import Dict, Generator, Optional, Tuple
import uuid

import magic

from aiocloudstorage.typed import FileLike
from aiocloudstorage.exceptions import InvalidBucketError,InvalidFileURLError,FileEmptyError
from aiocloudstorage import messages

_VALID_BUCKETNAME_REGEX = re.compile(
    '^[A-Za-z0-9][A-Za-z0-9\\.\\-\\_\\:]{1,61}[A-Za-z0-9]$')
_VALID_BUCKETNAME_STRICT_REGEX = re.compile(
    '^[a-z0-9][a-z0-9\\.\\-]{1,61}[a-z0-9]$')
_VALID_IP_ADDRESS = re.compile(
    r'^(\d+\.){3}\d+$')
_ALLOWED_HOSTNAME_REGEX = re.compile(
    '^((?!-)(?!_)[A-Z_\\d-]{1,63}(?<!-)(?<!_)\\.)*((?!_)(?!-)' +
    '[A-Z_\\d-]{1,63}(?<!-)(?<!_))$',
    re.IGNORECASE)


FILE_URL_REGEX = r'^([a-z0-9A-Z]{2,}):\/\/([^\/]+)\/(.{2,})$'
PROTOCOL_REGEX = re.compile(r'^(http|https|ssh|tcp)',re.IGNORECASE)
STORAGE_REGEX = re.compile(r'minio|fs|gcs|s3',re.IGNORECASE)

def is_file_url(url):
    mat = re.match(FILE_URL_REGEX,url) 
    if mat:
        groups = mat.groups()
        if re.search(STORAGE_REGEX,groups[0]):
            return True
    return False

def parse_file_url(url):
    if not is_file_url(url):
        raise InvalidFileURLError(messages.FILE_URL_INVALID%(url,))
    mat = re.match(FILE_URL_REGEX,url)
    if not mat:
        raise Exception("Unknown error occured while parsing url %s"%(url,))
    groups = mat.groups()
    return {'store':groups[0],'container':groups[1],'blob':groups[2]}
def random_filename(filename=None):
    basefolder,ext='',''
    if filename:
        try:
            base,ext = os.path.splitext(filename)
        except Exception as e:
            base,ext=filename,''
        basefolder = os.path.dirname(filename)
    name = uuid.uuid4().hex
    return os.path.join(basefolder,"%s%s"%(name,ext))

def is_valid_bucket_name(bucket_name, strict):
    """
    Check to see if the ``bucket_name`` complies with the
    restricted DNS naming conventions necessary to allow
    access via virtual-hosting style.
    :param bucket_name: Bucket name in *str*.
    :return: True if the bucket is valid. Raise :exc:`InvalidBucketError`
       otherwise.
    """
    # Verify bucket name is not empty
    bucket_name = str(bucket_name).strip()
    if bucket_name == '':
        raise InvalidBucketError('Bucket name cannot be empty.')

    # Verify bucket name length.
    if len(bucket_name) < 3:
        raise InvalidBucketError('Bucket name cannot be less than'
                                 ' 3 characters.')
    if len(bucket_name) > 63:
        raise InvalidBucketError('Bucket name cannot be greater than'
                                 ' 63 characters.')

    match = _VALID_IP_ADDRESS.match(bucket_name)
    if match:
        raise InvalidBucketError('Bucket name cannot be an ip address')

    unallowed_successive_chars = ['..', '.-', '-.']
    if any(x in bucket_name for x in unallowed_successive_chars):
        raise InvalidBucketError('Bucket name contains invalid '
                'successive chars ' + str(unallowed_successive_chars) + '.')

    if strict:
        match = _VALID_BUCKETNAME_STRICT_REGEX.match(bucket_name)
        if match is None or match.end() != len(bucket_name):
            raise InvalidBucketError('Bucket name contains invalid '
                                     'characters (strictly enforced).')

    match = _VALID_BUCKETNAME_REGEX.match(bucket_name)
    if match is None or match.end() != len(bucket_name):
        raise InvalidBucketError('Bucket name does not follow S3 standards.'
                                 ' Bucket: {0}'.format(bucket_name))

    return True

def clean_object_name(name):
    #remove front and back slash from name if not a path
    name = re.sub(r'^\/|\/$|\\/|\\','',name)
    name = re.sub(r'\/+','/',name)
    name = re.sub(r'[^a-z0-9A-Z\/\.\-_]','_',name)
    name = re.sub(r'_+','_',name)
    return name

async def transfer_stream(readstream,writestream,block_size:int=1024*1024*2):
    """
    assumes readstream(httpstream) is asyncio compatible but 
    writestream(filestream) is not as filestreams perform
    better in synmode. 
    See https://github.com/Tinche/aiofiles/issues/71
    write stream should be opened with wb
    """
    while True:
        chunk = await readstream.read(block_size)
        if not chunk:
            break
        writestream.write(chunk)

def read_in_chunks(file_object: FileLike,
                   block_size: int = 4096) -> Generator[bytes, None, None]:
    """Return a generator which yields data in chunks.

    Source: `read-file-in-chunks-ram-usage-read-strings-from-binary-file
    <https://stackoverflow.com/questions/17056382/
    read-file-in-chunks-ram-usage-read-strings-from-binary-files>`_

    :param file_object: File object to read in chunks.
    :type file_object: file object

    :param block_size: (optional) Chunk size.
    :type block_size: int

    :yield: The next chunk in file object.
    :yield type: `bytes`
    """
    for chunk in iter(lambda: file_object.read(block_size), b''):
        yield chunk


def file_checksum(filename: FileLike, hash_type: str = 'md5',
                  block_size: int = 4096) -> HASH:
    """Returns checksum for file.

    .. code-block:: python

        from aiocloudstorage.helpers import file_checksum

        picture_path = '/path/picture.png'
        file_checksum(picture_path, hash_type='sha256')
        # '03ef90ba683795018e541ddfb0ae3e958a359ee70dd4fccc7e747ee29b5df2f8'

    Source: `get-md5-hash-of-big-files-in-python <https://stackoverflow.com/
    questions/1131220/get-md5-hash-of-big-files-in-python>`_

    :param filename: File path or stream.
    :type filename: str or FileLike

    :param hash_type: Hash algorithm function name.
    :type hash_type:  str

    :param block_size: (optional) Chunk size.
    :type block_size: int

    :return: Hash of file.
    :rtype: :class:`_hashlib.HASH`

    :raise RuntimeError: If the hash algorithm is not found in :mod:`hashlib`.

    .. versionchanged:: 0.4
      Returns :class:`_hashlib.HASH` instead of `HASH.hexdigest()`.
    """
    try:
        file_hash = getattr(hashlib, hash_type)()
    except AttributeError:
        raise RuntimeError('Invalid or unsupported hash type: %s' % hash_type)

    if isinstance(filename, str):
        with open(filename, 'rb') as file_:
            for chunk in read_in_chunks(file_, block_size=block_size):
                file_hash.update(chunk)
    else:
        for chunk in read_in_chunks(filename, block_size=block_size):
            file_hash.update(chunk)
        # rewind the stream so it can be re-read later
        if filename.seekable():
            filename.seek(0)

    return file_hash

def check_file_not_empty(filename):
    if isinstance(filename, str):
        with open(filename,'rb') as f:
            chunk = f.read(1)
            f.seek(0)
            if not chunk:
                raise FileEmptyError(messages.FILE_EMPTY%(filename))
    elif hasattr(filename,'file'):
        "in case of fileupload in fast api the file is in file attr"
        filename.file.seek(0)
        chunk = filename.file.read(1)
        filename.file.seek(0)
        if not chunk:
            raise FileEmptyError(messages.FILE_EMPTY%(filename))
    elif hasattr(filename,'read'):
        chunk = filename.read(1)
        filename.seek(0)
        if not chunk:
            raise FileEmptyError(messages.FILE_EMPTY%(filename))
    


def validate_file_or_path(filename: FileLike) -> Optional[str]:
    """Return filename from file path or from file like object.

    Source: `rackspace/pyrax/object_storage.py <https://github.com/pycontribs/
    pyrax/blob/master/pyrax/object_storage.py>`_

    :param filename: File path or file like object.
    :type filename: str or file

    :return: Filename.
    :rtype: str or None

    :raise FileNotFoundError: If the file path is invalid.
    """
    if isinstance(filename, str):
        # Make sure it exists
        if not os.path.exists(filename):
            raise FileNotFoundError(filename)
        name = os.path.basename(filename)
    else:
        if hasattr(filename,'name'):
            name = os.path.basename(str(filename.name))
        elif hasattr(filename,'filename'):
            """
            uploaded file in fastapi has filename as name
            """
            name = filename.filename
        else:
            name = None
    return name


def file_content_type(filename: FileLike) -> Optional[str]:
    """Guess content type for file path or file like object.

    :param filename: File path or file like object.
    :type filename: str or file

    :return: Content type.
    :rtype: str or None
    """
    if isinstance(filename, str):
        if os.path.isfile(filename):
            content_type = magic.from_file(filename=filename, mime=True)
        else:
            content_type = mimetypes.guess_type(filename)[0]
    else:  # BufferedReader
        name = validate_file_or_path(filename)
        content_type = mimetypes.guess_type(name)[0]

    return content_type or ''


def parse_content_disposition(data: str) -> Tuple[Optional[str], Dict]:
    """Parse Content-Disposition header.

    Example: ::

        >>> parse_content_disposition('inline')
        ('inline', {})

        >>> parse_content_disposition('attachment; filename="foo.html"')
        ('attachment', {'filename': 'foo.html'})

    Source: `pyrates/multifruits <https://github.com/pyrates/multifruits>`_

    :param data: Content-Disposition header value.
    :type data: str

    :return: Disposition type and fields.
    :rtype: tuple
    """
    dtype = None
    params = {}
    length = len(data)
    start = 0
    end = 0
    i = 0
    quoted = False
    previous = ''
    field = None

    while i < length:
        c = data[i]
        if not quoted and c == ';':
            if dtype is None:
                dtype = data[start:end]
            elif field is not None:
                params[field.lower()] = data[start:end].replace('\\', '')
                field = None
            i += 1
            start = end = i
        elif c == '"':
            i += 1
            if not previous or previous != '\\':
                if not quoted:
                    start = i
                quoted = not quoted
            else:
                end = i
        elif c == '=':
            field = data[start:end]
            i += 1
            start = end = i
        elif c == ' ':
            i += 1
            if not quoted and start == end:  # Leading spaces.
                start = end = i
        else:
            i += 1
            end = i

        previous = c

    if i:
        if dtype is None:
            dtype = data[start:end].lower()
        elif field is not None:
            params[field.lower()] = data[start:end].replace('\\', '')

    return dtype, params
