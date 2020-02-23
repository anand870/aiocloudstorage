import sys
import pytest
import asyncio
import aiobotocore
from aiocloudstorage.drivers.minio import MinioDriver
from tests.settings import *

@pytest.fixture()
async def default_bucket():
    loop = asyncio.get_running_loop()
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client('s3', 
            region_name=MINIO_REGION,
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        ) as client:
        bucket_name = CONTAINER_PREFIX+'default'
        try:
            await client.create_bucket(Bucket=bucket_name)
        except:
            pass
    return bucket_name

@pytest.yield_fixture()
async def storage():
    loop = asyncio.get_running_loop()
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client('s3', 
            region_name=AMAZON_REGION,
            endpoint_url=AMAZON_ENDPOINT_URL,
            aws_access_key_id=AMAZON_KEY,
            aws_secret_access_key=AMAZON_SECRET
        ) as client:
        yield client
        resp = await client.list_buckets()
        buckets = resp.get('Buckets',[])
        for bucket_obj in buckets:
            if bucket_obj['Name'].startswith(CONTAINER_PREFIX):
                await client.delete_bucket(Bucket=bucket_obj['Name'])


@pytest.mark.asyncio
async def test_has_bucket_list(storage,default_bucket):
    s3=storage
    try:
        resp = await s3.list_buckets()
    except:
        pytest.fail("Bucket listing raises error")

    buckets = [bucket['Name'] for bucket in resp.get('Buckets',[])]
    assert (default_bucket in buckets) == True

@pytest.mark.asyncio
async def test_can_head_bucket(storage,default_bucket):
    s3=storage
    resp = await s3.head_bucket(Bucket=default_bucket)
    assert resp.get('ResponseMetadata',{}).get('HTTPStatusCode')==200


@pytest.mark.asyncio
async def test_raise_exception_on_non_existant_bucket(storage):
    s3=storage
    with pytest.raises(Exception) as e:
        buckets = await s3.head_bucket(Bucket='nonexist')
