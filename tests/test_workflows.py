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
@patch('oulibq.tasks.workflows.get_metadata')
@patch('oulibq.tasks.workflows.chain')
@patch('oulibq.tasks.workflows.group')
@patch('oulibq.tasks.workflows.find_bag')
@patch('oulibq.tasks.workflows.app.Task.request')
def test_replicate(mock_Task_request, mock_find_bag, mock_celery_group, mock_celery_chain, mock_get_metadata):
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
    validate_nas_files = patch('oulibq.tasks.workflows.validate_nas_files').start()
    copy_bag = patch('oulibq.tasks.workflows.copy_bag').start()
    validate_norfile_bag = patch('oulibq.tasks.workflows.validate_norfile_bag').start()
    upload_bag_s3 = patch('oulibq.tasks.workflows.upload_bag_s3').start()
    validate_s3_files = patch('oulibq.tasks.workflows.validate_s3_files').start()
    remove_nas_files = patch('oulibq.tasks.workflows.remove_nas_files').start()
    
    # Mock out the celery signatures of the tasks
    validate_nas = validate_nas_files.si(bag, nas_path).set(queue=queue_name)
    copy_to_norfile = copy_bag.si(bag, nas_path, norfile_path).set(queue=queue_name)
    validate_norfile = validate_norfile_bag.si(bag, norfile_path).set(queue=queue_name)
    upload_to_s3 = upload_bag_s3.si(bag, nas_path, s3_bucket, s3_key).set(queue=queue_name)
    validate_s3 = validate_s3_files.si(bag, nas_path, s3_bucket, s3_folder).set(queue=queue_name)
    remove_from_nas = remove_nas_files.si(bag).set(queue=queue_name)

    mock_find_bag.return_value = (nas_path, norfile_path, s3_bucket, s3_key, s3_folder)

    mock_Task_request.delivery_info.return_value = {'routing_key': queue_name}
    
    copy_metadata["locations"]["norfile"]["exists"] = False
    copy_metadata["locations"]["s3"]["exists"] = False
    mock_get_metadata.return_value = copy_metadata
    replicate(bag, project, department)
    assert mock_celery_chain.call_count == 3
    assert mock_celery_group.call_count == 1
    
    assert mock_celery_chain.called_with(
        validate_nas,
        mock_celery_group,
        remove_from_nas
    )
    assert mock_celery_group.called_with(
        mock_celery_chain,
        mock_celery_chain
    )
    # Brute force the nested calls to make sure the correct tasks are called
    # modifying the workflow will require changing the orders indicated in these indices
    assert mock_celery_chain.call_args_list[0][0][0] != upload_to_s3
    assert mock_celery_chain.call_args_list[0][0][0] == copy_to_norfile
    assert mock_celery_chain.call_args_list[0][0][1] == validate_norfile
    assert mock_celery_chain.call_args_list[1][0][0] != validate_nas
    assert mock_celery_chain.call_args_list[1][0][0] == upload_to_s3
    assert mock_celery_chain.call_args_list[1][0][1] == validate_s3
    assert mock_celery_chain.call_args_list[2][0][0] == validate_nas
    assert mock_celery_chain.call_args_list[2][0][1] == mock_celery_group()
    assert mock_celery_chain.call_args_list[2][0][2] == remove_from_nas

    assert mock_celery_group.call_args_list[0][0][0] == mock_celery_chain()
    assert mock_celery_group.call_args_list[0][0][1] == mock_celery_chain()
    
    # Reset the mock objects to test another cases
    mock_celery_chain.reset_mock()
    mock_celery_group.reset_mock()
    
    copy_metadata["locations"]["s3"]["exists"] = True
    copy_metadata["locations"]["s3"]["valid"] = False
    copy_metadata["locations"]["norfile"]["exists"] = True
    copy_metadata["locations"]["norfile"]["valid"] = False
    mock_get_metadata.return_value = copy_metadata
    replicate(bag, project, department)
    assert mock_celery_chain.call_count == 1
    assert mock_celery_group.call_count == 0
    
    assert mock_celery_chain.called_with(
        validate_nas,
        validate_norfile,
        validate_s3,
        remove_from_nas
    )
    
    mock_celery_chain.reset_mock()
    mock_celery_group.reset_mock()
    
    copy_metadata["locations"]["norfile"]["exists"] = True
    copy_metadata["locations"]["norfile"]["valid"] = False
    mock_get_metadata.return_value = copy_metadata
    replicate(bag, project, department)
    assert mock_celery_chain.call_count == 1
    assert mock_celery_group.call_count == 0
    
    assert mock_celery_chain.called_with(
        validate_nas,
        validate_norfile,
        remove_from_nas
    )
    
    mock_celery_chain.reset_mock()
    mock_celery_group.reset_mock()
    
    copy_metadata["locations"]["s3"]["exists"] = True
    copy_metadata["locations"]["s3"]["valid"] = False
    mock_get_metadata.return_value = copy_metadata
    replicate(bag, project, department)
    assert mock_celery_chain.call_count == 1
    assert mock_celery_group.call_count == 0
    
    assert mock_celery_chain.called_with(
        validate_nas,
        validate_s3,
        remove_from_nas
    ) 

    
