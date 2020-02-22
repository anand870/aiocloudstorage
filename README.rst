=============
Aio Cloud Storage
=============


`AioCloud Storage`_ is a Python +3.7 package which creates a unified API for the
cloud storage services: Amazon Simple Storage Service (S3), inspired by 
[lytics/cloudstorage](https://github.com/lytics/cloudstorage). Currently it supports
only local and minio drivers. Minio Drivers use aiobotocore for interaction with minio 
server, So same code can be used to interact with s3 but not tested yet(TODO).

Advantages to cloudstorage are:

* Full Python 3 support.
* Hassle Free file upload and download
* Pythonic! Iterate through all blobs in containers and all containers in
  storage using respective objects.

Usage
=====

Please see testcases for usage

Supported Services
==================

* `Minio Cloud Storage`_
* Local File System


Installation
============
Not hosted on pip. 
