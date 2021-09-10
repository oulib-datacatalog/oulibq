import pytest
from oulibq.tasks.utils import is_private, mmsid_exists, find_bag
from oulibq.tasks.config import bag_locations


def test_private_bags():
    assert is_private("shareok/test123") is True
    assert is_private("private/test123") is True
    assert is_private("preservation/test123") is True
    assert is_private("external-preservation/test123") is True

    assert is_private("share/test123") is False
    assert is_private("externalpreservation/test123") is False
    assert is_private("/shareok/test123") is False
    assert is_private("/preservation/test123") is False
    assert is_private("/external-preservation/test123") is False


def test_mmsid_exists():
    assert mmsid_exists("Tyler_2021_098765432") is True
    assert mmsid_exists("Tyler_2021_0987654321012") is True

    assert mmsid_exists("Tyler_2021_1234567") is False
    assert mmsid_exists("Tyler_2021_12345678901234567890") is False
    assert mmsid_exists("Tyler_2021") is False


def test_find_bag(tmpdir):
    private_dir = tmpdir / "private"
    private_dir.mkdir()
    bag_dir = private_dir / "test_bag"
    bag_dir.mkdir()
    test_file = bag_dir / "test.txt"
    test_file.write("testing...")

    norfile_dir = tmpdir / "norfile"
    norfile_dir.mkdir()

    bag_locations["nas"]["bagit"] = str(tmpdir)
    bag_locations["norfile"]["bagit"] = str(norfile_dir)

    assert find_bag("private/test_bag") == (
        bag_locations["nas"]["bagit"] + '/private/test_bag',
        bag_locations["norfile"]["bagit"],
        bag_locations["s3"]["bucket"],
        'private/private/test_bag',
        'private'
    )

