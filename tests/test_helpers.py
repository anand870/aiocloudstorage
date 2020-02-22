import pytest
import io

from aiocloudstorage.helpers import (
    file_checksum,
    file_content_type,
    parse_content_disposition,
    read_in_chunks,
    validate_file_or_path,
    is_valid_bucket_name,
    clean_object_name,
    random_filename,
    is_file_url,
    parse_file_url,
    check_file_not_empty,
)
from aiocloudstorage.exceptions import InvalidBucketError,InvalidFileURLError,FileEmptyError
from tests.settings import *
from tests.helpers import UploadFile


def test_read_in_chunks(binary_stream):
    block_size = 32
    binary_stream_size = os.fstat(binary_stream.fileno()).st_size
    total_chunks_read = round(binary_stream_size / block_size)

    data = read_in_chunks(binary_stream, block_size=block_size)
    assert sum(1 for _ in data) == total_chunks_read


def test_file_checksum_filename(text_filename):
    file_hash = file_checksum(text_filename, hash_type='md5', block_size=32)
    assert file_hash.hexdigest() == TEXT_MD5_CHECKSUM


def test_file_checksum_stream(binary_stream):
    file_hash = file_checksum(binary_stream, hash_type='md5', block_size=32)
    assert file_hash.hexdigest() == BINARY_MD5_CHECKSUM
    assert binary_stream.tell() == 0


def test_validate_file_or_path(text_filename, binary_stream):
    assert validate_file_or_path(text_filename) == TEXT_FILENAME
    assert validate_file_or_path(binary_stream) == BINARY_FILENAME


def test_file_content_type(text_filename, binary_stream):
    assert file_content_type(text_filename) == 'text/plain'
    assert file_content_type(binary_stream) == 'image/png'


@pytest.mark.parametrize("value,expected", [
    ('', (None, {})),
    ('inline', ('inline', {})),
    ('"inline"', ('inline', {})),
    ('inline; filename="foo.html"', ('inline', {'filename': 'foo.html'})),
    ('attachment', ('attachment', {})),
    ('"attachment"', ('attachment', {})),
    ('attachment; filename="foo.html"',
     ('attachment', {'filename': 'foo.html'})),
], ids=[
    'empty',
    'inline',
    'inline quoted',
    'inline with filename',
    'attachment',
    'attachment quoted',
    'attachment with filename',
])
def test_parse_content_disposition(value, expected):
    disposition, params = parse_content_disposition(value)
    assert disposition == expected[0]
    assert params == expected[1]

def test_is_valid_bucket_name():
    with pytest.raises(InvalidBucketError) as e:
        is_valid_bucket_name('abcCDE.-_:abc',False)
    with pytest.raises(InvalidBucketError) as e:
        is_valid_bucket_name('/abc',False)
    with pytest.raises(InvalidBucketError) as e:
        is_valid_bucket_name('abc/',False)==True

    assert is_valid_bucket_name('abci123-gre',False) == True
    with pytest.raises(InvalidBucketError) as e:
        is_valid_bucket_name('abc jdhgd',True)==True
    assert is_valid_bucket_name('abci123-gr.e',True) == True

def test_clean_object_name():
    assert clean_object_name('Test123')=='Test123'
    assert clean_object_name('Test_123')=='Test_123'
    assert clean_object_name('/abc123')=='abc123'
    assert clean_object_name('abc123/')=='abc123'
    assert clean_object_name('/abc123/defjhss/')=='abc123/defjhss'
    assert clean_object_name('/abc\\/123/defjhss/')=='abc123/defjhss'
    assert clean_object_name('/abc\\/123/defjhss/')=='abc123/defjhss'
    assert clean_object_name('/abc\\123/defjhss/')=='abc123/defjhss'
    assert clean_object_name('/abc\\123//defjhss/')=='abc123/defjhss'
    assert clean_object_name('123476365785686548568456486485645646548648658454.jpg')=='123476365785686548568456486485645646548648658454.jpg'
    assert clean_object_name('/abc\\123//defjhss/def.jpg')=='abc123/defjhss/def.jpg'
    assert clean_object_name('/abc\\123//def$#%@&#*$@:"jhss/def.jpg')=='abc123/def_jhss/def.jpg'

def test_random_filename():
    assert random_filename(None)
    assert random_filename('')
    assert random_filename('abc.jpg').endswith('.jpg')
    assert random_filename('abc/abc.jpg').endswith('.jpg')
    assert random_filename('abc/abc.jpg').startswith('abc/')
    assert random_filename('abc/abc.jpg')!='abc/'
    assert random_filename('abc/abc').startswith('abc/')
    assert random_filename('abc/abc').endswith('abc')==False

def test_is_file_url():
    assert is_file_url('fs://trash/abc.jpg')
    assert is_file_url('fs12://trash/abc/jhfdf$#122.jpg')
    assert is_file_url('minio://trash/abc/jhfdf$#122.jpg')
    assert is_file_url('MINIO://trash/abc/jhfdf$#122.jpg')
    assert is_file_url('S3://trash123/abc/jhfdf$#122.jpg')
    assert is_file_url('GCS://trash123/abc/jhfdf$#122.jpg')
    assert is_file_url('RCS://trash123/abc/jhfdf$#122.jpg')==False
    assert is_file_url('HTTP://trash123/abc/jhfdf$#122.jpg')==False
    assert is_file_url('SSh://trash123/abc/jhfdf$#122.jpg')==False
    assert is_file_url('/trash123/abc/jhfdf$#122.jpg') == False
    assert is_file_url('http122.jpg')==False

def test_parse_file_url():
    assert parse_file_url('fs://trash/abc.jpg') == {'store':'fs','container':'trash','blob':'abc.jpg'}
    assert parse_file_url('fs12://trash/abc/jhfdf$#122.jpg') == {'store':'fs12','container':'trash','blob':'abc/jhfdf$#122.jpg'}
    assert parse_file_url('minio://trash/abc/jhfdf$#122.jpg') == {'store':'minio','container':'trash','blob':'abc/jhfdf$#122.jpg'}
    assert parse_file_url('MINIO://trash/abc/jhfdf$#122.jpg') == {'store':'MINIO','container':'trash','blob':'abc/jhfdf$#122.jpg'}
    assert parse_file_url('S3://trash123/abc/jhfdf$#122.jpg') == {'store':'S3','container':'trash123','blob':'abc/jhfdf$#122.jpg'}
    assert parse_file_url('GCS://trash123/abc/jhfdf$#122.jpg') == {'store':'GCS','container':'trash123','blob':'abc/jhfdf$#122.jpg'}
    with pytest.raises(InvalidFileURLError) as err:
        parse_file_url('HTTP://trash123/abc/jhfdf$#122.jpg')
    with pytest.raises(InvalidFileURLError) as err:
        parse_file_url('SSh://trash123/abc/jhfdf$#122.jpg')
    with pytest.raises(Exception) as err:
        parse_file_url('HTTP://trash123/abc/jhfdf$#122.jpg')

def test_check_file_not_empty():
    with pytest.raises(FileEmptyError) as err:
        check_file_not_empty(io.BytesIO(b''))
    with pytest.raises(FileEmptyError) as err:
        check_file_not_empty(UploadFile('random'))

