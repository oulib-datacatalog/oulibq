import pytest
from copy import deepcopy

from oulibq.tasks.tasks import validate_nas_files
from oulibq.tasks.config import INVENTORY_METADATA

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

