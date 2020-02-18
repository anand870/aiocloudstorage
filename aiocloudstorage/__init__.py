"""Cloud Storage

:copyright: (c) 2017 by Scott Werner.
:license: MIT, see LICENSE for more details.
"""
import logging
import os
import asyncio
from enum import Enum, unique
from typing import Dict
import tempfile

from aiocloudstorage.base import Blob, Container, Driver
from aiocloudstorage.exceptions import CloudStorageError,CredentialsError
from aiocloudstorage.typed import FileLike
from aiocloudstorage.messages import STORAGE_NOT_ENABLED
from aiocloudstorage.helpers import parse_file_url,is_file_url

__all__ = [
    'Blob',
    'Container',
    'Driver',
    'DriverName',
    'get_driver',
    'get_driver_by_name',
]

__title__ = 'Cloud Storage'
__version__ = '0.10.0'
__author__ = 'Scott Werner'
__license__ = 'MIT'
__copyright__ = 'Copyright 2017-2018 Scott Werner'


@unique
class DriverName(Enum):
    """DriverName enumeration."""
    AZURE = 'AZURE'
    CLOUDFILES = 'CLOUDFILES'
    GOOGLESTORAGE = 'GOOGLESTORAGE'
    LOCAL = 'LOCAL'
    MINIO = 'MINIO'
    S3 = 'S3'


_DRIVER_IMPORTS = {
    #DriverName.AZURE: ('aiocloudstorage.drivers.microsoft', 'AzureStorageDriver'),
    #DriverName.CLOUDFILES: (
    #    'aiocloudstorage.drivers.rackspace', 'CloudFilesDriver'),
    #DriverName.GOOGLESTORAGE: ('aiocloudstorage.drivers.google',
    #                           'GoogleStorageDriver'),
    DriverName.LOCAL: ('aiocloudstorage.drivers.local', 'LocalDriver'),
    DriverName.MINIO: ('aiocloudstorage.drivers.minio', 'MinioDriver'),
    #DriverName.S3: ('aiocloudstorage.drivers.amazon', 'S3Driver'),
}
_m = {}
def _init_config():
    global _m
    _m={
        "confs":{},
        "default_store":None,
        "default_container":None,
        "storage_enabled":False
    }
def _check_storage_enabled():
    if not _m['storage_enabled']:
        raise CloudStorageError(STORAGE_NOT_ENABLED)

async def _check_driver_valid(conf):
    klass = conf['klass']
    driver = klass(**conf)
    try:
        async for container in driver.get_containers():
            break
    except Exception as err:
        raise CloudStorageError("Error Connecting to driver %s : %s"%(conf['driver'],str(err)))

async def _ensure_container(conf,container_name):
    """
    Ensure that default container exists in default store
    """
    klass = conf['klass']
    driver = klass(**conf)
    container = await driver.create_container(container_name)
    return container



async def configure(configuration):
    _init_config()
    if not configuration.get('STORAGE_ENABLED'):
        return False
    _m['storage_enabled'] = True
    if not configuration.get('STORAGE_CONFIG'):
        raise Exception("No storage configuration found in %s"%(str(configuration)))
    store_conf = configuration['STORAGE_CONFIG']
    for conf in store_conf:
        #need to check if at lease configuration is right if provided
        for key in ['name','endpoint','driver']:
            assert key in conf and conf[key],('%s key not found in config %s'%(key,conf))
        driver_name = conf['driver'].upper()
        try:
            driver_exists = DriverName[driver_name]
        except:
            raise Exception("Invalid driver name provided %s"%(driver_name))

        enable_key = "DRIVER_%s_ENABLED"%(conf['driver'],)
        driver_enabled = configuration.get(enable_key)

        if not driver_enabled:
            continue

        name = conf['name']
        conf['alias_name'] = name
        klass = get_driver_by_name(driver_name)
        conf['klass'] = klass
        await _check_driver_valid(conf)
        _m['confs'][name] = conf
    if len(_m['confs'])<=0:
        raise Exception("No storage driver has been installed.Please check storage configuration")

    default_container=configuration.get('DEFAULT_CONTAINER',None) 
    default_store = configuration.get('DEFAULT_STORE',None)
    _m['default_store'] = default_store
    _m['default_container'] = default_container
    if default_store is not None and default_store not in _m['confs']:
        raise Exception("Default Store %s not found in configuration or driver not enabled :final configuration %s"%(default_store,_m['confs']))
    if default_store and default_container:
        await _ensure_container(_m['confs'][default_store],default_container)
    return _m



async def _get_container(container_name,store_name,**kwargs) -> Container:
    container=kwargs.get('container',None)
    if container is not None:
        assert isinstance(container,Container),("Invalid container type")
    else:
        container_name = container_name or _m['default_container']
        store_name = store_name or _m['default_store']
        if not container_name and not _m['default_container']:
            raise CloudStorageError("container_name must be provided. No default container configured")
        elif not container_name:
            container_name = _m['default_container']

        if store_name and store_name not in _m['confs']:
            raise CloudStorageError("store name %s not configured"%(store_name))
        elif not store_name and _m['default_store'] is None:
            raise CloudStorageError("store_name must be provided. No default store configured")
        elif not store_name:
            store_name = _m['default_store']
        if not store_name or not container_name:
            raise CloudStorageError("Unknown error occured in getting container")
        conf = _m['confs'][store_name]
        klass = conf['klass']
        driver = klass(**conf)
        container = await driver.get_container(container_name)
    return container

async def download(fileurl,destfilename:str='auto',destpath:str=None,**kwargs):
    _check_storage_enabled()
    parsed = parse_file_url(fileurl)
    blob_name = parsed['blob']

    container = await _get_container(parsed['container'],parsed['store'],**kwargs)
    blob = await container.get_blob(blob_name)
    if not isinstance(destfilename,str) and hasattr(destfilename,'write'):
        if destpath is not None:
            raise Exception("destpath is invalid when providing stream")
        await blob.download(destfilename)
        #dont know the filepath here so return empty string
        try:
            return str(destfilename.name)
        except AttributeError as e:
            return ''

    if destfilename=='auto':
        #take filename from file url
        destfilename = os.path.basename(blob_name)
    elif destfilename!='auto' and destpath is None:
        #destfilename may be full filepath or a stream
        destpath = os.path.dirname(destfilename)
        destfilename = os.path.basename(destfilename)
    if destpath:
        #ensure directory exists
        try:
            if not os.path.exists(destpath):
                os.makedirs(destpath)
        except PermissionError as err:
            raise CredentialsError(str(err))
    if destpath is not None and destfilename:
        download_path = os.path.join(destpath,destfilename)
        await blob.download(download_path)
        return download_path
    else:
        #make own temporary file and return it
        with tempfile.NamedTemporaryFile(mode='w+b',delete=False) as dfile:
            await blob.download(dfile)
            return dfile.name
        
async def bulk_download(filedict:Dict,destfilename:str='auto',destpath:str=None,**kwargs):
    """
    filedict: a dictionary containing key and file. the returned
        dictionary will have the same key along with uploaded 
        file path
    """
    _check_storage_enabled()
    if not isinstance(filedict,dict):
        raise Exception("Expected dict but got %s"%(type(filedict),))
    if not len(filedict):
        return {}
    multi_container = kwargs.get('multi_container',True)
    if not multi_container:
        first_file_url = ''
        for key in filedict:
            first_file_url = filedict[key]
            break
        parsed = parse_file_url(first_file_url)
        container = await _get_container(parsed['container'],parsed['store'],**kwargs)
        kwargs['container'] = container

    tasks = []
    keys = []
    for key,fileurl in filedict.items():
        task = download(fileurl,destfilename,destpath,**kwargs)
        tasks.append(asyncio.create_task(task))
        keys.append(key)
    paths = await asyncio.gather(*tasks)
    file_paths = {}
    for index,key in enumerate(keys):
        file_paths[key]=paths[index]
    return file_paths

async def upload(filepath:FileLike,destfilename:str='random',destpath:str='',container_name=None,store_name=None,**kwargs):
    """
    destfilename:
        auto - name generated from filename
        random - random uuid
        <provided> - user rovided name
    """
    _check_storage_enabled()
    if not _m['storage_enabled']:
        raise CloudStorageError(STORAGE_NOT_ENABLED)
    container = await _get_container(container_name,store_name,**kwargs)
    blob = await container.upload_blob(filepath,destfilename,destpath)
    return blob

async def bulk_upload(filedict:Dict,destfilename:str='random',destpath:str='',container_name=None,store_name=None,**kwargs):
    """
    filedict: a dictionary containing key and file. the returned
        dictionary will have the same key along with uploaded 
        file url
    destfilename:
        auto - name generated from filename
        random - random uuid
        usekey - keys will be used as name
        <string> - user rovided filepath
        <steream> - user rovided filestream
    """
    _check_storage_enabled()
    if not isinstance(filedict,dict):
        raise Exception("Expected dict but got %s"%(type(filedict),))
    if not len(filedict):
        return {}
    tasks = []
    keys = []
    container = await _get_container(container_name,store_name,**kwargs)
    for key,_file in filedict.items():
        if destfilename=='usekey':
            destfilename = key
        task = upload(_file,destfilename,destpath,container=container)
        tasks.append(asyncio.create_task(task))
        keys.append(key)
    blobs = await asyncio.gather(*tasks)
    file_urls = {}
    for index,key in enumerate(keys):
        file_urls[key]=blobs[index].file_url
    return file_urls


def get_driver(driver: DriverName) -> Driver:
    """Get driver class by DriverName enumeration member.

    .. code-block:: python

        >>> from aiocloudstorage import DriverName, get_driver
        >>> driver_cls = get_driver(DriverName.LOCAL)
        <class 'aiocloudstorage.drivers.local.LocalDriver'>

    :param driver: DriverName member.
    :type driver: :class:`.DriverName`

    :return: DriverName driver class.
    :rtype: :class:`.AzureStorageDriver`, :class:`.CloudFilesDriver`,
      :class:`.GoogleStorageDriver`, :class:`.S3Driver`, :class:`.LocalDriver`,
      :class:`.MinioDriver`
    """
    if driver in _DRIVER_IMPORTS:
        mod_name, driver_name = _DRIVER_IMPORTS[driver]
        _mod = __import__(mod_name, globals(), locals(), [driver_name])
        return getattr(_mod, driver_name)

    raise CloudStorageError("Driver '%s' does not exist." % driver)


def get_driver_by_name(driver_name: str) -> Driver:
    """Get driver class by driver name.

    .. code-block:: python

        >>> from aiocloudstorage import get_driver_by_name
        >>> driver_cls = get_driver_by_name('LOCAL')
        <class 'aiocloudstorage.drivers.local.LocalDriver'>

    :param driver_name: Driver name.

        * `AZURE`
        * `CLOUDFILES`
        * `GOOGLESTORAGE`
        * `S3`
        * `LOCAL`
        * `MINIO`
    :type driver_name: str

    :return: DriverName driver class.
    :rtype: :class:`.AzureStorageDriver`, :class:`.CloudFilesDriver`,
      :class:`.GoogleStorageDriver`, :class:`.S3Driver`, :class:`.LocalDriver`,
      :class:`.MinioDriver`
    """
    driver = DriverName[driver_name]
    return get_driver(driver)


# Set up logging to ``/dev/null`` like a library is supposed to.
logging.getLogger('aiocloudstorage').addHandler(logging.NullHandler())
