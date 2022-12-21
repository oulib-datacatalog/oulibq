from tempfile import tempdir
import pytest
from copy import deepcopy
from os.path import exists
import shutil
import os

from celery.exceptions import Ignore

from oulibq.tasks.tasks import validate_nas_files, remove_nas_files, move_bag_nas, move_bag_s3
from oulibq.tasks.config import BAG_LOCATIONS, INVENTORY_METADATA, SuspiciousLocation, NotFullyReplicated, \
     BagDoesNotExist, NotABag, BagAlreadyExists

import bagit
from six import PY2

if PY2:
    from mock import MagicMock, Mock, patch
else:
    from unittest.mock import MagicMock, Mock, patch

@patch('oulibq.tasks.tasks.upsert_metadata')
@patch('oulibq.tasks.tasks.get_metadata')
def test_validate_nas_files(mock_get_metadata, mock_upsert, tmpdir):
    bag_dir = tmpdir / 'test_bag'
    bag_dir.mkdir()
    test_file = bag_dir / 'test.txt'
    test_file.write('testing...')
    bag = bagit.make_bag(str(bag_dir), checksums=['md5', 'sha256'])
   
    mock_get_metadata.return_value = deepcopy(INVENTORY_METADATA)
    copy_metadata = deepcopy(INVENTORY_METADATA)
    copy_metadata['locations']['nas']['exists'] = True
    copy_metadata['locations']['nas']['location'] = str(bag_dir)
    validate_nas_files('test_bag', str(tmpdir))
    mock_get_metadata.assert_called_with('test_bag')
    mock_upsert.assert_called_with(copy_metadata)
    
@patch('oulibq.tasks.tasks.get_metadata')
@patch('shutil.rmtree')
@patch('oulibq.tasks.tasks.upsert_metadata')
@patch('oulibq.tasks.tasks.touch_file')
def test_remove_nas_files(mock_touch, mock_upsert,mock_rmtree, mock_get_metadata, tmpdir): 
    copy_metadata = deepcopy(INVENTORY_METADATA)
    copy_metadata['locations']['norfile']['valid'] = True
    copy_metadata['locations']['s3']['valid'] = True
    copy_metadata['locations']['nas']['location'] = "/"
    mock_get_metadata.return_value = copy_metadata
    with pytest.raises(SuspiciousLocation):
        remove_nas_files('test_bag')
        
    copy_metadata = deepcopy(INVENTORY_METADATA)
    copy_metadata['locations']['norfile']['valid'] = True
    copy_metadata['locations']['s3']['valid'] = True
    copy_metadata['locations']['nas']['location'] = str(tmpdir)
    mock_get_metadata.return_value = copy_metadata
    with pytest.raises(BagDoesNotExist):
        remove_nas_files('test_bag')    

    copy_metadata = deepcopy(INVENTORY_METADATA)
    copy_metadata['locations']['norfile']['valid'] = True
    copy_metadata['locations']['s3']['valid'] = False
    mock_get_metadata.return_value = copy_metadata
    with pytest.raises(NotFullyReplicated):
        remove_nas_files('test_bag')

    copy_metadata = deepcopy(INVENTORY_METADATA)
    copy_metadata['locations']['norfile']['valid'] = False
    copy_metadata['locations']['s3']['valid'] = True
    mock_get_metadata.return_value = copy_metadata
    with pytest.raises(NotFullyReplicated):
        remove_nas_files('test_bag')
        
    bagdir = tmpdir / 'bagdir'
    bagdir.mkdir()
    print('this is bag directory: ', str(bagdir))
    copy_metadata = deepcopy(INVENTORY_METADATA)
    copy_metadata['locations']['norfile']['valid'] = True
    copy_metadata['locations']['s3']['valid'] = True
    copy_metadata['locations']['nas']['exists'] = True
    copy_metadata['locations']['nas']['location'] = str(bagdir)
    mock_get_metadata.return_value = copy_metadata
    
    assert exists(str(bagdir)) == True
    mock_rmtree.return_value = True
    mock_upsert.return_value = True
    assert copy_metadata['locations']['nas']['place_holder'] == False 
    remove_nas_files('test_bag')
    assert copy_metadata['locations']['nas']['exists'] == False
    assert copy_metadata['locations']['nas']['place_holder'] == True

    
def test_move_bag_nas_non_bag(tmpdir):
    source_dir = tmpdir / 'some_path'
    source_dir.mkdir()
    dest_dir = tmpdir / 'new_path'

    with pytest.raises(NotABag):
        move_bag_nas(str(source_dir), str(dest_dir))


def test_move_bag_nas_existing_destination(tmpdir):
    source_dir = tmpdir / 'some_path'
    source_dir.mkdir()
    bagit_file = source_dir / 'bagit.txt'
    bagit_file.write('testing')
    dest_dir = tmpdir / 'new_path'
    dest_dir.mkdir()

    with pytest.raises(BagAlreadyExists):
        move_bag_nas(str(source_dir), str(dest_dir))


@patch('oulibq.tasks.tasks.call')
def test_move_bag_call_returned_error(mock_call, tmpdir):
    source_dir = tmpdir / 'some_path'
    source_dir.mkdir()
    bagit_file = source_dir / 'bagit.txt'
    bagit_file.write('testing')
    dest_dir = tmpdir / 'new_path'

    mock_call.return_value = 1
    with pytest.raises(Ignore):
        move_bag_nas(str(source_dir), str(dest_dir))


def test_move_bag_nas(tmpdir):
    source_dir = tmpdir / 'some_path'
    source_dir.mkdir()
    bagit_file = source_dir / 'bagit.txt'
    bagit_file.write('testing')
    dest_dir = tmpdir / 'new_path'

    move_bag_nas(str(source_dir), str(dest_dir))

    assert not os.path.exists(str(source_dir))
    assert os.path.exists(str(dest_dir))


def test_move_bag_s3(s3_client, s3_test_bucket):
    bucket = os.environ['DEFAULT_BUCKET']
    source_bag_name = "source/TEST_BAG"
    destination_bag_name = "source/NEW_BAG"
    s3_test_bucket.put_object(Bucket=bucket, Key='{0}/bagit.txt'.format(source_bag_name), Body='TEST')

    mock_bag_location = patch.dict('oulibq.tasks.tasks.BAG_LOCATIONS', {"s3": {"bucket": bucket}})
    mock_bag_location.start()
    mock_query_metadata = patch('oulibq.tasks.tasks.query_metadata')
    mock_query_metadata.start()
    mock_update_metadata_subfield = patch('oulibq.tasks.tasks.update_metadata_subfield')
    mock_update_metadata_subfield.start()

    move_bag_s3(source_path=source_bag_name, destination_path=destination_bag_name)

    patch.stopall()


def test_move_bag_s3_not_bag(s3_client, s3_test_bucket):
    bucket = os.environ['DEFAULT_BUCKET']
    source_bag_name = "source/TEST_BAG"
    destination_bag_name = "source/NEW_BAG"
    s3_test_bucket.put_object(Bucket=bucket, Key='{0}/somefile'.format(source_bag_name), Body='TEST')

    mock_bag_location = patch.dict('oulibq.tasks.tasks.BAG_LOCATIONS', {"s3": {"bucket": bucket}})
    mock_bag_location.start()

    with pytest.raises(NotABag):
        move_bag_s3(source_path=source_bag_name, destination_path=destination_bag_name)

    patch.stopall()


def test_move_bag_s3_destination_exists(s3_client, s3_test_bucket):
    bucket = os.environ['DEFAULT_BUCKET']
    source_bag_name = "source/TEST_BAG"
    destination_bag_name = "source/NEW_BAG"
    s3_test_bucket.put_object(Bucket=bucket, Key='{0}/bagit.txt'.format(source_bag_name), Body='TEST')
    s3_test_bucket.put_object(Bucket=bucket, Key='{0}/bagit.txt'.format(destination_bag_name), Body='TEST')

    mock_bag_location = patch.dict('oulibq.tasks.tasks.BAG_LOCATIONS', {"s3": {"bucket": bucket}})
    mock_bag_location.start()

    with pytest.raises(BagAlreadyExists):
        move_bag_s3(source_path=source_bag_name, destination_path=destination_bag_name)

    patch.stopall()
