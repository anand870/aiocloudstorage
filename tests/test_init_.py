import pytest
from aiocloudstorage.drivers.local import LocalDriver
from aiocloudstorage.drivers.minio import MinioDriver
from aiocloudstorage import configure
from tests.settings import *
from tests.helpers import random_container_name, uri_validator

@pytest.yield_fixture(scope='function')
async def minio_storage():
    storage = MinioDriver(
               MINIO_ENDPOINT,
               MINIO_ACCESS_KEY,
               MINIO_SECRET_KEY
            )
    yield storage
    async for container in storage.get_containers():
        if container.name.startswith(CONTAINER_PREFIX):
            async for blob in container.get_blobs():
                await blob.delete()
            await container.delete()

@pytest.yield_fixture(scope='function')
async def local_storage():
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
async def test_init_configure_name_missing(config):
    config['STORAGE_CONFIG'][0].pop('name')
    with pytest.raises(Exception) as e:
        resp = await configure(config)

@pytest.mark.asyncio
async def test_init_configure_driver_missing(config):
    config['STORAGE_CONFIG'][0].pop('driver')
    with pytest.raises(Exception) as e:
        resp = await configure(config)

@pytest.mark.asyncio
async def test_init_configure_endpoint_missing(config):
    config['STORAGE_CONFIG'][0].pop('endpoint')
    with pytest.raises(Exception) as e:
        resp = await configure(config)

@pytest.mark.asyncio
async def test_init_configure_driver_invalid(config):
    config['STORAGE_CONFIG'][0]['driver']='unknown'
    config['DEFAULT_STORE']='unknown'
    with pytest.raises(Exception) as e:
        resp = await configure(config)

@pytest.mark.asyncio
async def test_init_configure_store_disabled(config):
    config['STORAGE_CONFIG'][0]['driver']='unknown'
    config['STORAGE_ENABLED']=False
    try:
        resp = await configure(config)
    except:
        pytest.fail("Disabled Store should not raise error")


@pytest.mark.asyncio
async def test_init_configure_driver_disabled(config):
    config = config
    config['STORAGE_CONFIG'][0]['driver']='unknown'
    config['STORAGE_ENABLED']=False
    try:
        resp = await configure(config)
    except:
        pytest.fail("Disabled Store should not raise error")

@pytest.mark.asyncio
async def test_init_configure_driver_conf_invalid(minio_store_config):
    minio_store_config['STORAGE_CONFIG'][0]['key']='123'
    with pytest.raises(Exception) as e:
        resp = await configure(minio_store_config)
    print(e)


@pytest.mark.asyncio
async def test_init_configure_default_container_created_local(local_storage,store_config):
    store,config = local_storage,store_config

    container_name = config['DEFAULT_CONTAINER']
    await configure(config)
    try:
        await store.get_container(container_name)
    except:
        pytest.fail("Default Container in local not created")

@pytest.mark.asyncio
async def test_init_configure_default_container_created_minio(minio_storage,minio_store_config):
    store,config = minio_storage,minio_store_config

    container_name = config['DEFAULT_CONTAINER']
    await configure(config)
    try:
        await store.get_container(container_name)
    except:
        pytest.fail("Default Container in local not created")
