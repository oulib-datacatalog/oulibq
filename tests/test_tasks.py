from tempfile import tempdir
import pytest
from copy import deepcopy
from os.path import exists
import shutil
import os

#from sqlalchemy import false

from oulibq.tasks.tasks import validate_nas_files, remove_nas_files
from oulibq.tasks.config import INVENTORY_METADATA, SuspiciousLocation, NotFullyReplicated, BagDoesNotExist


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

    
        