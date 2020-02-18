import io
from random import randint
from tempfile import mkstemp,mkdtemp
import shutil

import pytest

from tests.helpers import random_container_name
from tests.settings import *

ROOT = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture()
def storage():
    pass

@pytest.fixture()
async def configured_container():
    pass

# noinspection PyShadowingNames
@pytest.fixture()
async def container(storage):
    container_name = random_container_name()
    container = await storage.create_container(container_name)
    yield container


@pytest.fixture(scope='session')
def text_filename():
    return os.path.join(ROOT, 'data', TEXT_FILENAME)


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def text_stream(text_filename):
    with open(text_filename, 'rb') as text_stream:
        yield text_stream


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
async def text_blob(container, text_filename):
    text_blob = await container.upload_blob(text_filename)

    yield text_blob
    try:
        await text_blob.delete()
    except:
        pass


# noinspection PyShadowingNames
@pytest.fixture(scope='session')
def binary_filename():
    return os.path.join(ROOT, 'data', BINARY_FILENAME)


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def binary_stream(binary_filename):
    with open(binary_filename, 'rb') as binary_stream:
        yield binary_stream

# noinspection PyShadowingNames
@pytest.yield_fixture(scope='function')
async def binary_blob(container, binary_filename):
    binary_blob = await container.upload_blob(binary_filename)
    yield binary_blob
    try:
        await binary_blob.delete()
    except:
        pass

@pytest.fixture(scope='function')
async def binary_blob_list(container):
    blobs = []
    count=10
    for i in range(count):
        data = b'\x01'*1024*randint(1,10)
        fileio = io.BytesIO(data)
        blob = await container.upload_blob(fileio)
        blobs.append(blob)
    yield blobs
    for blob in blobs:
        try:
            await blob.delete()
        except:
            pass

@pytest.fixture(scope='function')
async def random_blob_list(storage):
    count=5
    blobs = []
    for i in range(count):
        container_name = random_container_name()
        container=await storage.create_container(container_name)
        data = b'\x01'*1024*randint(1,10)
        fileio = io.BytesIO(data)
        blob = await container.upload_blob(fileio)
        blobs.append(blob)
    yield blobs
    for blob in blobs:
        try:
            await blob.delete()
        except:
            pass

@pytest.fixture(scope='function')
def temp_file():
    _, path = mkstemp(prefix=CONTAINER_PREFIX)
    yield path
    os.remove(path)

@pytest.fixture(scope='function')
def temp_dir():
    path = mkdtemp(prefix=CONTAINER_PREFIX)
    yield path
    try:
        shutil.rmtree(path)
    except OSError as err:
        pass

@pytest.fixture(scope='function')
def random_dirpath():
    path = os.path.join('/tmp',random_container_name())
    yield path
    try:
        shutil.rmtree(path)
    except OSError as err:
        pass

@pytest.fixture(scope='function')
def random_filepath(random_dirpath):
    path = os.path.join(random_dirpath,random_container_name()+'.txt')
    yield path
    try:
        os.remove(path)
    except OSError as err:
        pass



@pytest.fixture(scope='function')
def store_config():
    return {
        "STORAGE_CONFIG":[ 
            {   
                "name": LOCAL_NAME,
                "endpoint":LOCAL_ENDPOINT,
                "driver":"LOCAL",
            },  
        ],
        "DEFAULT_STORE":"fs",
        "DRIVER_LOCAL_ENABLED": True,
        "DEFAULT_CONTAINER": random_container_name(),
        "STORAGE_ENABLED":True,
    }

@pytest.fixture(scope='function')
def minio_store_config():
    return {
        "STORAGE_CONFIG":[ 
            {   
                "name": MINIO_NAME,
                "endpoint":MINIO_ENDPOINT,
                "driver":"MINIO",
                "secret":MINIO_SECRET_KEY,
                "key":MINIO_ACCESS_KEY,
                "region":MINIO_REGION
            },  
        ],
        "DEFAULT_STORE":"minio",
        "DRIVER_MINIO_ENABLED": True,
        "DEFAULT_CONTAINER": random_container_name(),
        "STORAGE_ENABLED":True,
    }

@pytest.fixture(params=['minio','local'])
def config(request):
    if request.param == 'minio':
        return request.getfixturevalue('minio_store_config')
    else:
        return request.getfixturevalue('store_config')
