from oulibq.tasks.config import INVENTORY_METADATA
from copy import deepcopy


def test_copy_dict_no_change():
    new_metadata = deepcopy(INVENTORY_METADATA)
    new_metadata['bag'] = "testing"
    assert new_metadata['bag'] != INVENTORY_METADATA['bag']
    assert INVENTORY_METADATA['bag'] == ''


def test_sub_dict_change_in_dict():
    new_metadata = deepcopy(INVENTORY_METADATA)
    assert new_metadata['locations']['nas']['exists'] is False

    nas_metadata = new_metadata['locations']['nas']
    assert nas_metadata['exists'] is False

    nas_metadata['exists'] = True
    assert nas_metadata['exists'] is True
    assert new_metadata['locations']['nas']['exists'] is True
    assert INVENTORY_METADATA['locations']['nas']['exists'] is False
