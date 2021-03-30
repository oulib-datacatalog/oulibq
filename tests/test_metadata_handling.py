from oulibq.tasks.config import inventory_metadata
from copy import deepcopy


def test_copy_dict_no_change():
    new_metadata = deepcopy(inventory_metadata)
    new_metadata['bag'] = "testing"
    assert new_metadata['bag'] != inventory_metadata['bag']
    assert inventory_metadata['bag'] == ''


def test_sub_dict_change_in_dict():
    new_metadata = deepcopy(inventory_metadata)
    assert new_metadata['locations']['nas']['exists'] is False

    nas_metadata = new_metadata['locations']['nas']
    assert nas_metadata['exists'] is False

    nas_metadata['exists'] = True
    assert nas_metadata['exists'] is True
    assert new_metadata['locations']['nas']['exists'] is True
    assert inventory_metadata['locations']['nas']['exists'] is False
