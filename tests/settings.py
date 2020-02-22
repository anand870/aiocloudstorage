import hashlib
import os
from tempfile import mkdtemp
from time import time

from prettyconf import Configuration

from aiocloudstorage.structures import CaseInsensitiveDict

config = Configuration()

# Append epoch to prevent test runs from clobbering each other.
CONTAINER_PREFIX = 'cloud-storage-test'
SECRET = hashlib.sha1(os.urandom(128)).hexdigest()
SALT = hashlib.sha1(os.urandom(128)).hexdigest()

TEXT_FILENAME = 'flask.txt'
TEXT_STREAM_FILENAME = 'flask-stream.txt'
TEXT_FORM_FILENAME = 'flask-form.txt'
TEXT_MD5_CHECKSUM = '2a5a634f5c8d931350e83e41c9b3b0bb'
TEXT_FILE_EXTENSION = '.txt'
TEXT_NESTED_UPLOAD_PATH = 'uploads/images'
TEXT_NESTED_UPLOAD_NAME = 'uploads/images/flask.txt'

FILE_URL='%s://%s/%s' 

BINARY_FILENAME = 'avatar.png'
BINARY_FILENAME_EXTENSION = '.png'
BINARY_FORM_FILENAME = 'avatar-form.png'
BINARY_STREAM_FILENAME = 'avatar-stream.png'
BINARY_MD5_CHECKSUM = '2f907a59924ad96b7478074ed96b05f0'

ZERO_BYTE_FILE_HASH = 'd41d8cd98f00b204e9800998ecf8427e'

# Azure: Does not support dashes.
# Rackspace: Converts underscores to dashes.
# Minio: Capitalizes the key.
BINARY_OPTIONS = CaseInsensitiveDict({
    'content_type': 'image/png',
    'content_disposition': 'attachment; filename=avatar-attachment.png',
    'cache_control': 'max-age=84600',
})

AMAZON_KEY = config('AMAZON_KEY', default=None)
AMAZON_SECRET = config('AMAZON_SECRET', default=None)
AMAZON_REGION = config('AMAZON_REGION', default='us-east-1')
AMAZON_ENDPOINT_URL= config('AMAZON_ENDPOINT_URL', default=None)

AZURE_ACCOUNT_NAME = config('AZURE_ACCOUNT_NAME', default=None)
AZURE_ACCOUNT_KEY = config('AZURE_ACCOUNT_KEY', default=None)

GOOGLE_CREDENTIALS = config('GOOGLE_CREDENTIALS', default=None,
                            cast=lambda path: os.path.abspath(path) if path else None)

LOCAL_ENDPOINT = config('LOCAL_ENDPOINT', default=mkdtemp(prefix='cloud-storage-test-'))
LOCAL_SECRET = config('LOCAL_SECRET', default='local-storage-secret')
LOCAL_NAME = "fs"

RACKSPACE_KEY = config('RACKSPACE_KEY', default=None)
RACKSPACE_SECRET = config('RACKSPACE_SECRET', default=None)
RACKSPACE_REGION = config('RACKSPACE_REGION', default='IAD')

MINIO_ENDPOINT = config('MINIO_ENDPOINT', default='s3.amazonaws.com')
MINIO_ACCESS_KEY = config('MINIO_ACCESS_KEY', default=AMAZON_KEY)
MINIO_SECRET_KEY = config('MINIO_SECRET_KEY', default=AMAZON_SECRET)
MINIO_REGION = config('MINIO_REGION', default=AMAZON_REGION)
MINIO_NAME = "minio"
