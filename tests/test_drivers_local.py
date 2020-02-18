import io
import asyncio
import pytest
from aiocloudstorage import Container
from aiocloudstorage.drivers.local import LocalDriver
from aiocloudstorage.exceptions import CloudStorageError,NotFoundError
from aiocloudstorage.helpers import file_checksum
from tests.helpers import random_container_name, uri_validator
from tests.settings import *

@pytest.yield_fixture()
async def storage():
    storage = LocalDriver(
               LOCAL_ENDPOINT,
               LOCAL_SECRET
            )
    yield storage
    async for container in storage.get_containers():
        if container.name.startswith(CONTAINER_PREFIX):
            async for blob in container.get_blobs():
                await blob.delete()
            await container.delete()


@pytest.mark.asyncio
async def test_driver_get_container(storage,container):
    _container = await storage.get_container(container.name)
    assert isinstance(_container,Container)
    assert _container.name == container.name

@pytest.mark.asyncio
async def test_driver_get_container_raises_not_found_error(storage):
    with pytest.raises(NotFoundError) as e:
        container = await storage.get_container('nonexist')

@pytest.mark.asyncio
async def test_driver_create_container(storage):
    container_name = CONTAINER_PREFIX+'mycontainer'
    container = await storage.create_container(container_name)
    assert container.name == container_name
    try:
        created_container = await storage.get_container(container_name)
    except:
        pytest.fail('Container not created')

@pytest.mark.asyncio
async def test_driver_create_container_existing(storage,container):
    try:
        new_container = await storage.create_container(container.name)
    except:
        pytest.fail('create_container should not raise error if it already exist')

@pytest.mark.asyncio
async def test_driver_delete_container(storage,container):
    resp = await storage.delete_container(container)
    with pytest.raises(NotFoundError) as e:
        nocontainer = await storage.get_container(container.name)
    assert resp==True

@pytest.mark.asyncio
async def test_driver_delete_container_invalid(storage):
    try:
        resp = await storage.delete_container(Container('nonexist',storage))
        assert resp == False
    except:
        pytest.fail("delete_container should not raise error")

@pytest.mark.asyncio
async def test_container_delete(storage):
    container_name = random_container_name()
    container = await storage.create_container(container_name)
    await container.delete()
    with pytest.raises(NotFoundError) as e:
        nocontainer = await storage.get_container(container.name)

@pytest.mark.asyncio
async def test_container_upload_path(container,text_filename):
    blob = await container.upload_blob(text_filename,blob_name=TEXT_FILENAME)
    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.name == TEXT_FILENAME

@pytest.mark.asyncio
async def test_container_upload_nested_path(container,text_filename):
    blob = await container.upload_blob(text_filename,blob_name=TEXT_FILENAME,blob_path=TEXT_NESTED_UPLOAD_PATH)

    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.name == TEXT_NESTED_UPLOAD_NAME

@pytest.mark.asyncio
async def test_container_upload_nested_path_with_front_slash(container,text_filename):
    slash_path = '/'+TEXT_NESTED_UPLOAD_PATH
    blob = await container.upload_blob(text_filename,blob_name=TEXT_FILENAME,blob_path=slash_path)

    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.name == TEXT_NESTED_UPLOAD_NAME

@pytest.mark.asyncio
async def test_container_upload_nested_path_auto_name(container,text_filename):
    blob = await container.upload_blob(text_filename,blob_name='auto',blob_path=TEXT_NESTED_UPLOAD_PATH)

    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.name == TEXT_NESTED_UPLOAD_NAME

@pytest.mark.asyncio
async def test_container_upload_nested_path_random_name(container,text_filename):
    blob = await container.upload_blob(text_filename,blob_name='random',blob_path=TEXT_NESTED_UPLOAD_PATH)

    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.name.startswith(TEXT_NESTED_UPLOAD_PATH) == True

@pytest.mark.asyncio
async def test_container_upload_path_auto_name(container,text_filename):
    blob = await container.upload_blob(text_filename)
    assert blob.name == TEXT_FILENAME

@pytest.mark.asyncio
async def test_container_upload_path_random_name(container,text_filename):
    blob = await container.upload_blob(text_filename,blob_name='random')
    assert blob.name != TEXT_FILENAME
    assert blob.name.endswith(TEXT_FILE_EXTENSION) == True

@pytest.mark.asyncio
async def test_container_upload_stream(container,binary_stream):
    blob = await container.upload_blob(binary_stream,blob_name=BINARY_STREAM_FILENAME,**BINARY_OPTIONS)
    assert blob.name == BINARY_STREAM_FILENAME
    assert blob.checksum == BINARY_MD5_CHECKSUM

@pytest.mark.asyncio
async def test_container_upload_zero_byte_stream(container):
    blob = await container.upload_blob(io.BytesIO(b''),blob_name=BINARY_STREAM_FILENAME,**BINARY_OPTIONS)
    assert blob.name == BINARY_STREAM_FILENAME
    #assert blob.checksum == BINARY_MD5_CHECKSUM

@pytest.mark.asyncio
async def test_container_upload_zero_byte_stream_without_name(container):
    blob = await container.upload_blob(io.BytesIO(b''),blob_name='auto',**BINARY_OPTIONS)
    assert blob.checksum == ZERO_BYTE_FILE_HASH

@pytest.mark.asyncio
async def test_container_get_blob(container,text_filename):
    blob = await container.upload_blob(text_filename,blob_name=TEXT_FILENAME)
    newblob = await container.get_blob(blob.name)
    assert newblob.name == blob.name

@pytest.mark.asyncio
async def test_container_get_blob_by_url(container,text_filename):
    blob = await container.upload_blob(text_filename,blob_name=TEXT_FILENAME)
    newblob = await container.get_blob(blob.file_url)
    assert newblob.name == blob.name
@pytest.mark.asyncio
async def test_container_get_blob_invalid(container,text_filename):
    with pytest.raises(NotFoundError) as e:
        newblob = await container.get_blob('notablob')

@pytest.mark.asyncio
async def test_blob_file_url_on_create(container,text_filename):
    blob = await container.upload_blob(text_filename,blob_name=TEXT_FILENAME)
    assert blob.file_url == FILE_URL%('fs',container.name,TEXT_FILENAME)

@pytest.mark.asyncio
async def test_blob_file_url_on_get(container,text_filename):
    text_blob = await container.upload_blob(text_filename)
    fetch_blob = await container.get_blob(TEXT_FILENAME)
    assert fetch_blob.file_url == FILE_URL%('fs',container.name,TEXT_FILENAME)

@pytest.mark.asyncio
async def test_blob_download_path(binary_blob, temp_file):
    await binary_blob.download(temp_file)
    hash_type = binary_blob.driver.hash_type
    download_hash = file_checksum(temp_file, hash_type=hash_type)
    assert download_hash.hexdigest() == BINARY_MD5_CHECKSUM


@pytest.mark.asyncio
async def test_blob_download_stream(binary_blob, temp_file):
    with open(temp_file, 'wb') as download_file:
        await binary_blob.download(download_file)
    hash_type = binary_blob.driver.hash_type
    download_hash = file_checksum(temp_file, hash_type=hash_type)
    assert download_hash.hexdigest() == BINARY_MD5_CHECKSUM

