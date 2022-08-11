from copy import deepcopy
from requests import patch
import pytest
from oulibq.tasks.config import INVENTORY_METADATA
from oulibq.tasks.workflows import replicate, managed_replication
from oulibq.tasks.config import BAG_LOCATIONS
import pytest
import bagit
from six import PY2

if PY2:
    from mock import MagicMock, Mock, patch
else:
    from unittest.mock import MagicMock, Mock, patch

#@pytest.mark.skip(reason="failing to mock self.request.delivery_info")
@patch('oulibq.tasks.workflows.get_metadata')
@patch('oulibq.tasks.workflows.app.Task.request')
@patch('oulibq.tasks.workflows.chain')
@patch('oulibq.tasks.workflows.group')
@patch('oulibq.tasks.workflows.find_bag')
def test_replicate(mock_find_bag, mock_celery_group, mock_celery_chain, mock_task, mock_get_metadata):
    copy_metadata = deepcopy(INVENTORY_METADATA)
    bag = "test_bag"
    project = "test_project"
    department = "test_department"
    nas_path = "test_nas_path"
    norfile_path = "test_norfile_path"
    s3_bucket = "test_s3_bucket"
    s3_key = "test_s3_key"
    s3_folder = "test_s3_folder"
    queue_name = "test_queue_name"

    # Mock out the celery tasks
    validate_nas_files = MagicMock()
    copy_bag = MagicMock()
    validate_norfile_bag = MagicMock()
    upload_bag_s3 = MagicMock()
    validate_s3_files = MagicMock()
    remove_nas_files = MagicMock()


    # Mock out the celery signatures of the tasks
    validate_nas = validate_nas_files.si(bag, nas_path).set(queue=queue_name)
    copy_to_norfile = copy_bag.si(bag, nas_path, norfile_path).set(queue=queue_name)
    validate_norfile = validate_norfile_bag.si(bag, norfile_path).set(queue=queue_name)
    upload_to_s3 = upload_bag_s3.si(bag, nas_path, s3_bucket, s3_key).set(queue=queue_name)
    validate_s3 = validate_s3_files.si(bag, nas_path, s3_bucket, s3_folder).set(queue=queue_name)
    remove_from_nas = remove_nas_files.si(bag).set(queue=queue_name)

    mock_find_bag.return_value = (nas_path, norfile_path, s3_bucket, s3_key, s3_folder)
    
    #FIXME: This does not work.
    mock_task.delivery_info.return_value =  {'routing_key': queue_name}
    
    copy_metadata["locations"]["norfile"]["exists"] = True
    copy_metadata["locations"]["norfile"]["valid"] = False
    mock_get_metadata.return_value = copy_metadata
    
    replicate(bag, project, department)
    upload_to_s3.assert_called()
    remove_from_nas.assert_called()
    validate_nas.assert_called()