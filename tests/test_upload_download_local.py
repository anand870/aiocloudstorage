import os
import pytest
from aiocloudstorage.drivers.local import LocalDriver
from tests.settings import *
from tests.helpers import random_container_name, uri_validator,binary_iostreams
from aiocloudstorage.exceptions import CloudStorageError,NotFoundError,InvalidFileURLError
from aiocloudstorage import configure,upload,bulk_upload,download,bulk_download
from aiocloudstorage.helpers import file_checksum,parse_file_url


@pytest.fixture()
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

@pytest.fixture()
async def container(request,storage,store_config):
    param = None if not hasattr(request,'param') else request.param
    print(param)
    if param == 'nds': #no default store
        store_config.pop('DEFAULT_STORE')
    elif param == 'ndc': #no default container
        store_config.pop('DEFAULT_CONTAINER')
    elif param=='nd': #no default
        store_config.pop('DEFAULT_CONTAINER')
        store_config.pop('DEFAULT_STORE')
    elif param == 'sd': #store disabled
        store_config['STORAGE_ENABLED']=False

    conf = await configure(store_config)
    container_name = store_config.get('DEFAULT_CONTAINER',random_container_name())
    container = await storage.create_container(container_name)
    return container

@pytest.mark.asyncio
async def test_upload_path(container,text_filename):
    blob = await upload(text_filename,'auto')
    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.name == TEXT_FILENAME
    assert blob.file_url == FILE_URL%(LOCAL_NAME,container.name,TEXT_FILENAME)


@pytest.mark.asyncio
async def test_upload_nested_path(container,text_filename):
    blob = await upload(text_filename,'auto',destpath=TEXT_NESTED_UPLOAD_PATH)
    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.name == TEXT_NESTED_UPLOAD_NAME

@pytest.mark.asyncio
async def test_upload_auto_name(container,text_filename):
    blob = await upload(text_filename,'auto')
    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.name == TEXT_FILENAME
    assert blob.file_url == FILE_URL%(LOCAL_NAME,container.name,TEXT_FILENAME)

@pytest.mark.asyncio
async def test_upload_random_name(container,text_filename):
    blob = await upload(text_filename,'random')
    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.name != TEXT_FILENAME
    assert blob.file_url == FILE_URL%(LOCAL_NAME,container.name,blob.name)

@pytest.mark.asyncio
async def test_upload_specified_name(container,text_filename):
    name = random_container_name()+TEXT_FILENAME
    blob = await upload(text_filename,name)
    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.name == name
    assert blob.file_url == FILE_URL%(LOCAL_NAME,container.name,name)

@pytest.mark.asyncio
async def test_upload_stream(container,binary_stream):
    blob = await upload(binary_stream,destfilename=BINARY_STREAM_FILENAME)
    assert blob.name == BINARY_STREAM_FILENAME
    assert blob.checksum == BINARY_MD5_CHECKSUM


@pytest.mark.asyncio
@pytest.mark.parametrize('container',['sd'],indirect=True)
async def test_upload_store_disabled(container,text_filename):
    with pytest.raises(CloudStorageError) as err:
        blob = await upload(text_filename,store_name=LOCAL_NAME,container_name=container.name)


@pytest.mark.asyncio
async def test_upload_container_invalid(container,text_filename):
    with pytest.raises(NotFoundError) as err:
        blob = await upload(text_filename,container_name=random_container_name())

@pytest.mark.asyncio
@pytest.mark.parametrize('container',['nd','nds','ndc','sd'],indirect=True)
async def test_upload_no_default(container,text_filename):
    with pytest.raises(CloudStorageError) as err:
        blob = await upload(text_filename)

@pytest.mark.asyncio
@pytest.mark.parametrize('container',['ndc'],indirect=True)
async def test_upload_no_default_container(container,text_filename):
    blob = await upload(text_filename,container_name=container.name)
    assert blob.checksum == TEXT_MD5_CHECKSUM
    assert blob.file_url == FILE_URL%(LOCAL_NAME,container.name,blob.name)

@pytest.mark.asyncio
async def test_bulk_upload_no_file(container):
    files = await bulk_upload({})
    assert files== {}

@pytest.mark.asyncio
async def test_bulk_upload(container):
    filecount = 10
    iostreams = binary_iostreams(filecount)
    destpath = random_container_name()
    files = await bulk_upload(iostreams,destpath='/'+destpath)
    assert isinstance(files,dict)
    assert len(files) == filecount
    hash_type = container.driver.hash_type
    for key,fileurl in files.items():
        iostreams[key].seek(0)
        download_hash = file_checksum(iostreams[key],hash_type=hash_type)
        blob = await container.get_blob(fileurl)
        assert blob.name.startswith(destpath)
        assert blob.checksum == download_hash.hexdigest()

@pytest.mark.asyncio
async def test_download_invalid_file_url():
    with pytest.raises(InvalidFileURLError) as err:
        filepath = await download('http://www.google.com/myfile.json')
@pytest.mark.asyncio
@pytest.mark.parametrize('container',['sd'],indirect=True)
async def test_download_storage_disabled(binary_blob):
    with pytest.raises(CloudStorageError) as err:
        filepath = await download(binary_blob.file_url)

@pytest.mark.asyncio
async def test_download_container_in_url_invalid(binary_blob):
    with pytest.raises(NotFoundError) as err:
        parsed = parse_file_url(binary_blob.file_url)
        invalid_url = "%s://%s/%s"%(parsed['store'],random_container_name(),parsed['blob'])
        filepath = await download(invalid_url)


@pytest.mark.asyncio
async def test_download_no_destpath_auto(binary_blob,random_filepath):
    try:
        filepath = await download(binary_blob.file_url,destfilename=random_filepath)
    except:
        pytest.fail("Should have treated destfilename as relative or absolute path")

@pytest.mark.asyncio
async def test_download_destpath_not_exist(binary_blob,random_dirpath):
    try:
        filepath = await download(binary_blob.file_url,destpath=random_dirpath)
    except:
        pytest.fail("Should auto create destination if not exist")
    assert os.path.isdir(random_dirpath)

@pytest.mark.asyncio
async def test_download_file_path(binary_blob,temp_file):
    filepath = await download(binary_blob.file_url,destfilename=temp_file)
    hash_type = binary_blob.driver.hash_type
    download_hash = file_checksum(filepath, hash_type=hash_type)
    assert download_hash.hexdigest() == binary_blob.checksum

@pytest.mark.asyncio
async def test_download_stream(binary_blob,temp_file):
    with open(temp_file,'wb') as download_file:
        await download(binary_blob.file_url,download_file)
    hash_type = binary_blob.driver.hash_type
    download_hash = file_checksum(temp_file, hash_type=hash_type)
    assert download_hash.hexdigest() == binary_blob.checksum

@pytest.mark.asyncio
async def test_download_without_destination(binary_blob):
    download_file = await download(binary_blob.file_url)
    hash_type = binary_blob.driver.hash_type
    download_hash = file_checksum(download_file, hash_type=hash_type)
    assert download_hash.hexdigest() == binary_blob.checksum

@pytest.mark.asyncio
async def test_bulk_download_no_file(container):
    path_dict = await bulk_download({})
    assert path_dict == {}

@pytest.mark.asyncio
async def test_bulk_download(binary_blob_list):
    blobs_dict = {k:v for k,v in enumerate(binary_blob_list)}
    fileurls_dict = {k:v.file_url for k,v in enumerate(binary_blob_list)}
    count = len(blobs_dict)
    path_dict = await bulk_download(fileurls_dict)
    assert len(path_dict) == count
    for key,blob in blobs_dict.items():
        hash_type = blob.driver.hash_type
        assert os.path.isfile(path_dict[key])
        download_hash = file_checksum(path_dict[key], hash_type=hash_type)
        assert download_hash.hexdigest() == blob.checksum

@pytest.mark.asyncio
async def test_bulk_download_multi_container(random_blob_list):
    blobs_dict = {k:v for k,v in enumerate(random_blob_list)}
    fileurls_dict = {k:v.file_url for k,v in enumerate(random_blob_list)}
    count = len(blobs_dict)
    path_dict = await bulk_download(fileurls_dict)
    assert len(path_dict) == count
    for key,blob in blobs_dict.items():
        hash_type = blob.driver.hash_type
        assert os.path.isfile(path_dict[key])
        download_hash = file_checksum(path_dict[key], hash_type=hash_type)
        assert download_hash.hexdigest() == blob.checksum




