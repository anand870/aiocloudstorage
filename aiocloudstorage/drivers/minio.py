"""Minio Simple Storage Service (Minio) Driver."""
import os
import logging
import asyncio
import warnings
from typing import Any, Dict, Iterable, List  # noqa: F401
from urllib.parse import quote, urljoin

import aiobotocore
from botocore.exceptions import ClientError, ParamValidationError, WaiterError
from aiocloudstorage.utils import camelize, underscore

from aiocloudstorage import Blob, Container, Driver, messages
from aiocloudstorage.exceptions import (
    CloudStorageError,
    CredentialsError,
    IsNotEmptyError,
    NotFoundError,
)
from aiocloudstorage.helpers import file_content_type, validate_file_or_path,transfer_stream,is_valid_bucket_name,clean_object_name
from aiocloudstorage.typed import (
    ContentLength,
    ExtraOptions,
    FileLike,
    FormPost,
    MetaData,
)

print(__name__)

__all__ = ['MinioDriver']

logger = logging.getLogger(__name__)

class Bucket(object):
    def __init__(self,Name,CreationDate=None):
        self.name = Name
        self.creation_date = CreationDate

class MinioDriver(Driver):
    """Driver for interacting with Amazon Simple Storage Service (Minio).

    .. code-block:: python

        from aiocloudstorage.drivers.amazon import MinioDriver

        storage = MinioDriver(key='<my-aws-access-key-id>',
                   secret='<my-aws-secret-access-key>',
                   region='us-east-1')
        # <Driver: Minio us-east-1>

    References:

    * `Boto 3 Docs <https://boto3.amazonaws.com/v1/documentation/api/
      latest/index.html>`_
    * `Amazon S3 REST API Introduction
      <https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html>`_

    :param key: AWS Access Key ID.
    :type key: str

    :param secret: AWS Secret Access Key.
    :type secret: str

    :param region: (optional) Region to connect to. Defaults to `us-east-1`.
    :type region: str

    :param kwargs: (optional) Extra driver options.
    :type kwargs: dict
    """
    name = 'Minio'
    hash_type = 'md5'
    url = 'https://aws.amazon.com/s3/'

    def __init__(self, endpoint:str, key: str, secret: str = None, region: str = 'us-east-1',alias_name="minio",
                 **kwargs: Dict) -> None:
        region = region.lower()
        self.endpoint = endpoint
        super().__init__(key=key, secret=secret, region=region, alias_name=alias_name,**kwargs)

        self._loop = kwargs.get('loop',None)
        self._session = None
        #self._session = boto3.Session(aws_access_key_id=key,
        #                              aws_secret_access_key=secret,
        #                              region_name=region)

        # session required for loading regions list
        #if region not in self.regions:
        #    raise CloudStorageError(messages.REGION_NOT_FOUND % region)
    def s3(self,loop=None):
        """
        Usage
        async with self.s3() as s3:
            s3.dosomething
        """
        client  = self.session.create_client('s3',
                region_name=self.region,
                endpoint_url=self.endpoint,
                aws_access_key_id=self.key,
                aws_secret_access_key=self.secret
            )
        return client

    @property
    def session(self):
        if not self._session:
            loop = self._loop or asyncio.get_running_loop()
            self._session = aiobotocore.get_session(loop=loop)
        return self._session

    async def _object_summary(self,bucket_name:str,blob_name:str) -> Dict:
        try:
            async with self.s3() as s3:
                resp = await s3.head_object(Bucket=bucket_name,Key=blob_name)
        except ClientError as err:
            error_code = int(err.response['Error']['Code'])
            if error_code == 404:
                raise NotFoundError(messages.BLOB_NOT_FOUND %
                                    (blob_name,bucket_name))

            raise CloudStorageError('%s: %s' % (
                err.response['Error']['Code'],
                err.response['Error']['Message']))
        resp.pop('ResponseMetadata')
        return resp

    @staticmethod
    def _normalize_parameters(params: Dict[str, str],
                              normalizers: Dict[str, str]) -> Dict[str, str]:
        normalized = params.copy()

        for key, value in params.items():
            normalized.pop(key)
            if not value:
                continue

            key_inflected = camelize(underscore(key),
                                     uppercase_first_letter=True)
            # Only include parameters found in normalizers
            key_overrider = normalizers.get(key_inflected.lower())
            if key_overrider:
                normalized[key_overrider] = value

        return normalized
    def _make_bucket(self,info:Dict) -> Bucket:
        return Bucket(**info)

    async def _list_buckets(self):
        async with self.s3() as s3:
            resp = await s3.list_buckets()
            for info in resp.get('Buckets',[]):
                yield self._make_bucket(info)

    async def _get_bucket(self, bucket_name: str, validate: bool = True):
        """Get a Minio bucket.

        :param bucket_name: The Bucket's name identifier.
        :type bucket_name: str

        :param validate: If True, verify that the bucket exists.
        :type validate: bool

        :return: Minio bucket resource object.
        :rtype: :class:`boto3.s3.Bucket`

        :raises NotFoundError: If the bucket does not exist.
        :raises CloudStorageError: Boto 3 client error.
        """

        if validate:
            try:
                async with self.s3() as s3:
                    response = await s3.head_bucket(Bucket=bucket_name)
                    logger.debug('response=%s', response)
            except ClientError as err:
                error_code = int(err.response['Error']['Code'])
                if error_code == 404:
                    raise NotFoundError(messages.CONTAINER_NOT_FOUND %
                                        bucket_name)

                raise CloudStorageError('%s: %s' % (
                    err.response['Error']['Code'],
                    err.response['Error']['Message']))

        return Bucket(bucket_name)

    def _make_blob(self, container: Container, object_summary) -> Blob:
        """Convert Minio Object Summary to Blob instance.

        :param container: The container that holds the blob.
        :type container: :class:`.Container`

        :param object_summary: Minio object summary.
        :type object_summary: :class:`boto3.s3.ObjectSummary`

        :return: A blob object.
        :rtype: :class:`.Blob`

        :raise NotFoundError: If the blob object doesn't exist.
        """
        try:
            name = object_summary['Key']
            #: etag wrapped in quotes
            checksum = etag = object_summary['ETag'].replace('"','')
            if 'Size' in object_summary:
                size = object_summary['Size']
            elif 'ContentLength' in object_summary:
                size = object_summary['ContentLength']
            else:
                raise Exception('No size key in response')

            #acl = object_summary.Acl()
            acl = None
            meta_data = object_summary.get('Metadata',{})
            content_disposition = object_summary.get('ContentDisposition',None)
            content_type = object_summary.get('ContentType',None)
            cache_control = object_summary.get('CacheControl',None)
            #meta_data = object_summary.meta.data.get('Metadata', {})
            #content_disposition = object_summary.meta.data.get(
            #    'ContentDisposition', None)
            #content_type = object_summary.meta.data.get('ContentType', None)
            #cache_control = object_summary.meta.data.get('CacheControl', None)
            modified_at = object_summary.get('LastModified',None)
            created_at = None
            expires_at = None  # TODO: FEATURE: Delete at / expires at
        except Exception as err:
            raise CloudStorageError('Invalid object summary %s'%(str(err),))

        return Blob(name=name, checksum=checksum, etag=etag, size=size,
                    container=container, driver=self, acl=acl,
                    meta_data=meta_data,
                    content_disposition=content_disposition,
                    content_type=content_type, cache_control=cache_control,
                    created_at=created_at, modified_at=modified_at,
                    expires_at=expires_at)

    def _make_container(self, bucket: Bucket) -> Container:
        """Convert Minio Bucket to Container.

        :param bucket: Minio bucket object.
        :type bucket: :class:`boto3.s3.Bucket`

        :return: The container if it exists.
        :rtype: :class:`.Container`
        """
        return Container(name=bucket.name, driver=self,
                         meta_data=None)

    async def get_container(self, container_name: str, validate:bool=True) -> Container:
        bucket = await self._get_bucket(container_name,validate=True)
        return self._make_container(bucket)

    async def get_containers(self):
        async for bucket in self._list_buckets():
            yield self._make_container(bucket)

    async def get_blobs(self,container: Container):
        try:
            async with self.s3() as s3:
                resp = await s3.list_objects_v2(Bucket=container.name)
        except ClientError as err:
            raise CloudStorageError('%s: %s' % (
                err.response['Error']['Code'],
                err.response['Error']['Message']))
        for obj_summary in resp.get('Contents',[]):
            blob = self._make_blob(container,obj_summary)
            yield blob

    async def create_container(self,container_name:str ,acl : str=None):
        is_valid_bucket_name(container_name,strict=True)
        try:
            async with self.s3() as s3:
                await s3.create_bucket(Bucket=container_name)
        except ClientError as err:
            pass
        bucket = await self._get_bucket(container_name,validate=False)
        return self._make_container(bucket)

    async def delete_container(self, container: Container) -> None:
        try:
            async with self.s3() as s3:
                await s3.delete_bucket(Bucket=container.name)
        except ClientError as err:
            error_code = err.response['Error']['Code']
            if error_code == 'BucketNotEmpty':
                raise IsNotEmptyError(messages.CONTAINER_NOT_EMPTY %
                                      container.name)
            elif error_code != 'NoSuchBucket':
                raise
            return False
        return True

    async def upload_blob(self, 
            container: Container, 
            filename: FileLike,
            blob_name: str, 
            blob_path: str = '', 
            acl: str = None,
            meta_data: MetaData = None, 
            content_type: str = '',
            content_disposition: str = None, 
            cache_control: str = None,
            chunk_size: int = 1024,
            extra: ExtraOptions = None
        ) -> Blob:
        meta_data = {} if meta_data is None else meta_data
        extra = {} if extra is None else extra

        extra_args = self._normalize_parameters(extra, self._PUT_OBJECT_KEYS)

        #config = boto3.s3.transfer.TransferConfig(io_chunksize=chunk_size)

        # Default arguments
        extra_args.setdefault('Metadata', meta_data)
        extra_args.setdefault('StorageClass', 'STANDARD')

        if acl:
            extra_args.setdefault('ACL', acl.lower())

        if cache_control:
            extra_args.setdefault('CacheControl', cache_control)

        if content_disposition:
            extra_args['ContentDisposition'] = content_disposition

        blob_name = blob_name or validate_file_or_path(filename)
        blob_name = os.path.join(blob_path,blob_name)
        blob_name =clean_object_name(blob_name) 
        # Boto uses application/octet-stream by default
        if not content_type:
            if isinstance(filename, str):
                # TODO: QUESTION: Any advantages between filename vs blob_name?
                extra_args['ContentType'] = file_content_type(filename)
            else:
                extra_args['ContentType'] = file_content_type(blob_name)
        else:
            extra_args['ContentType'] = content_type

        logger.debug('extra_args=%s', extra_args)

        async with self.s3() as s3:
            if isinstance(filename, str):
                with open(filename,'rb') as f:
                    await s3.put_object(Key=blob_name,Body=f,Bucket=container.name,**extra_args)
            elif hasattr(filename,'file'):
                #fastapi Upload file has file inside fileobject
                await s3.put_object(Key=blob_name,Body=filename.file,Bucket=container.name,**extra_args)
            else:
                await s3.put_object(Key=blob_name,Body=filename,Bucket=container.name,**extra_args)

        return await self.get_blob(container, blob_name)

    async def get_blob(self, container: Container, blob_name: str) -> Blob:
        object_summary = await self._object_summary(container.name,blob_name)
        object_summary['Key'] = blob_name
        return self._make_blob(container, object_summary)



    async def download_blob(self, blob: Blob,
                      destination: FileLike) -> None:
        async with self.s3() as s3:
            resp = await s3.get_object(Bucket=blob.container.name,Key=blob.name)
            if isinstance(destination, str):
                with open(destination,"wb") as f:
                    destination = f
                    await transfer_stream(resp['Body'],destination)
            else:
                await transfer_stream(resp['Body'],destination)

    def patch_blob(self, blob: Blob) -> None:
        raise NotImplementedError

    async def delete_blob(self, blob: Blob) -> None:
        # Required parameters
        params = {
            'Bucket': blob.container.name,
            'Key': blob.name,
        }
        logger.debug('params=%s', params)

        try:
            async with self.s3() as s3:
                response = await s3.delete_object(**params)
                logger.debug('response=%s', response)
        except ClientError as err:
            error_code = int(err.response['Error']['Code'])
            if error_code != 200 or error_code != 204:
                raise NotFoundError(messages.BLOB_NOT_FOUND % (
                    blob.name, blob.container.name))
            raise

    def blob_cdn_url(self, blob: Blob) -> str:
        container_url = self.container_cdn_url(blob.container)
        blob_name_cleaned = quote(blob.name)

        blob_path = '%s/%s' % (container_url, blob_name_cleaned)
        url = urljoin(container_url, blob_path)
        return url

    def generate_container_upload_url(self, container: Container,
                                      blob_name: str,
                                      expires: int = 3600, acl: str = None,
                                      meta_data: MetaData = None,
                                      content_disposition: str = None,
                                      content_length: ContentLength = None,
                                      content_type: str = None,
                                      cache_control: str = None,
                                      extra: ExtraOptions = None) -> FormPost:
        meta_data = {} if meta_data is None else meta_data
        extra = {} if extra is None else extra
        extra_norm = self._normalize_parameters(extra, self._POST_OBJECT_KEYS)

        conditions = []  # type: List[Any]
        fields = {}  # type: Dict[Any, Any]

        if acl:
            conditions.append({'acl': acl})
            fields['acl'] = acl

        headers = {
            'Content-Disposition': content_disposition,
            'Content-Type': content_type,
            'Cache-Control': cache_control,
        }
        for header_name, header_value in headers.items():
            if not header_value:
                continue

            fields[header_name.lower()] = header_value
            conditions.append(['eq', '$' + header_name, header_value])

        # Add content-length-range which is a tuple
        if content_length:
            min_range, max_range = content_length
            conditions.append(['content-length-range', min_range, max_range])

        for meta_name, meta_value in meta_data.items():
            meta_name = self._OBJECT_META_PREFIX + meta_name
            fields[meta_name] = meta_value
            conditions.append({meta_name: meta_value})

        # Add extra conditions and fields
        for extra_name, extra_value in extra_norm.items():
            fields[extra_name] = extra_value
            conditions.append({extra_name: extra_value})

        return self.s3.meta.client.generate_presigned_post(
            Bucket=container.name,
            Key=blob_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=int(expires))

    def generate_blob_download_url(self, blob: Blob, expires: int = 3600,
                                   method: str = 'GET',
                                   content_disposition: str = None,
                                   extra: ExtraOptions = None) -> str:
        extra = extra if extra is not None else {}
        params = self._normalize_parameters(extra, self._GET_OBJECT_KEYS)

        # Required parameters
        params['Bucket'] = blob.container.name
        params['Key'] = blob.name

        # Optional
        if content_disposition:
            params['ResponseContentDisposition'] = content_disposition

        logger.debug('params=%s', params)
        return self.s3.meta.client.generate_presigned_url(
            ClientMethod='get_object', Params=params, ExpiresIn=int(expires),
            HttpMethod=method.lower())

    _OBJECT_META_PREFIX = 'x-amz-meta-'  # type: str

    #: `Minio.Client.generate_presigned_post
    #: <http://boto3.readthedocs.io/en/latest/reference/services/s3.html
    #: #Minio.Client.generate_presigned_post>`_
    _POST_OBJECT_KEYS = {
        'acl': 'acl',
        'cachecontrol': 'Cache-Control',
        'contenttype': 'Content-Type',
        'contentdisposition': 'Content-Disposition',
        'contentencoding': 'Content-Encoding',
        'expires': 'Expires',
        'successactionredirect': 'success_action_redirect',
        'redirect': 'redirect',
        'successactionstatus': 'success_action_status',
        'xamzmeta': 'x-amz-meta-',
    }

    #: `#Minio.Client.get_object
    #: <http://boto3.readthedocs.io/en/latest/reference/services/s3.html
    #: #Minio.Client.get_object>`_
    _GET_OBJECT_KEYS = {
        'bucket': 'Bucket',
        'ifmatch': 'IfMatch',
        'ifmodifiedsince': 'IfModifiedSince',
        'ifnonematch': 'IfNoneMatch',
        'ifunmodifiedsince': 'IfUnmodifiedSince',
        'key': 'Key',
        'range': 'Range',
        'responsecachecontrol': 'ResponseCacheControl',
        'responsecontentdisposition': 'ResponseContentDisposition',
        'responsecontentencoding': 'ResponseContentEncoding',
        'responsecontentlanguage': 'ResponseContentLanguage',
        'responsecontenttype': 'ResponseContentType',
        'responseexpires': 'ResponseExpires',
        'versionid': 'VersionId',
        'ssecustomeralgorithm': 'SSECustomerAlgorithm',
        'ssecustomerkey': 'SSECustomerKey',
        'requestpayer': 'RequestPayer',
        'partnumber': 'PartNumber',
        # Extra keys to standarize across all drivers
        'cachecontrol': 'ResponseCacheControl',
        'contentdisposition': 'ResponseContentDisposition',
        'contentencoding': 'ResponseContentEncoding',
        'contentlanguage': 'ResponseContentLanguage',
        'contenttype': 'ResponseContentType',
        'expires': 'ResponseExpires',
    }

    #: `Minio.Client.put_object
    #: <http://boto3.readthedocs.io/en/latest/reference/services/s3.html
    #: #Minio.Client.put_object>`_
    _PUT_OBJECT_KEYS = {
        'acl': 'ACL',
        'body': 'Body',
        'bucket': 'Bucket',
        'cachecontrol': 'CacheControl',
        'contentdisposition': 'ContentDisposition',
        'contentencoding': 'ContentEncoding',
        'contentlanguage': 'ContentLanguage',
        'contentlength': 'ContentLength',
        'contentmd5': 'ContentMD5',
        'contenttype': 'ContentType',
        'expires': 'Expires',
        'grantfullcontrol': 'GrantFullControl',
        'grantread': 'GrantRead',
        'grantreadacp': 'GrantReadACP',
        'grantwriteacp': 'GrantWriteACP',
        'key': 'Key',
        'metadata': 'Metadata',
        'serversideencryption': 'ServerSideEncryption',
        'storageclass': 'StorageClass',
        'websiteredirectlocation': 'WebsiteRedirectLocation',
        'ssecustomeralgorithm': 'SSECustomerAlgorithm',
        'ssecustomerkey': 'SSECustomerKey',
        'ssekmskeyid': 'SSEKMSKeyId',
        'requestpayer': 'RequestPayer',
        'tagging': 'Tagging',
    }

    #: `Minio.Client.delete_object
    #: <http://boto3.readthedocs.io/en/latest/reference/services/s3.html
    #: #Minio.Client.delete_object>`_
    _DELETE_OBJECT_KEYS = {
        'bucket': 'Bucket',
        'key': 'Key',
        'mfa': 'MFA',
        'versionid': 'VersionId',
        'requestpayer': 'RequestPayer',
    }

    #: `Minio.Bucket.create
    #: <http://boto3.readthedocs.io/en/latest/reference/services/s3.html
    #: #Minio.Bucket.create>`_
    _POST_CONTAINER_KEYS = {
        'acl': 'ACL',
        'bucket': 'Bucket',
        'createbucketconfiguration': 'CreateBucketConfiguration',
        'locationconstraint': 'LocationConstraint',
        'grantfullcontrol': 'GrantFullControl',
        'grantread': 'GrantRead',
        'grantreadacp': 'GrantReadACP',
        'grantwrite': 'GrantWrite',
        'grantwriteacp': 'GrantWriteACP',
    }
