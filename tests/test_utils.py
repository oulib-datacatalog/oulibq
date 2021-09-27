from datetime import datetime, timedelta
from os import utime
from sys import version_info
import time

import pytest
import bagit

from oulibq.tasks.utils import is_private, mmsid_exists, find_bag, is_bag_valid, is_tombstone, is_older_than
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


def test_is_bag_valid(tmpdir):
    bag_dir = tmpdir / "test_bag"
    bag_dir.mkdir()
    test_file = bag_dir / "test.txt"
    test_file.write("testing...")
    bag = bagit.make_bag(str(bag_dir), checksums=["md5", "sha256"])

    assert bag.is_valid() is True
    assert is_bag_valid(str(bag_dir)) is True

    # adding a file to the bag makes it invalid
    added_file = bag_dir / "data" / "added.txt"
    added_file.write("This should cause the bag to be invalid")

    bag = bagit.Bag(str(bag_dir))
    assert bag.is_valid() is False
    assert is_bag_valid(str(bag_dir)) is False


def test_is_tombstone(tmpdir):
    empty_file = tmpdir / "tyler_2021"
    empty_file.write("")

    assert is_tombstone(str(empty_file)) is True
    assert is_tombstone(str(tmpdir)) is False


def test_is_older_than(tmpdir):
    empty_file = tmpdir / "tyler_2021"
    empty_file.write("")

    assert is_older_than(str(empty_file), 2) is False

    # Change file timestamp to two days ago
    now = datetime.now()
    two_days_ago = now - timedelta(days=2)
    if version_info.major == 2:  # handle Python 2.x
        timestamp_2_days_ago = time.mktime(two_days_ago.timetuple())
    else:
        timestamp_2_days_ago = two_days_ago.timestamp()
    utime(str(empty_file), (timestamp_2_days_ago, timestamp_2_days_ago))
    assert is_older_than(str(empty_file), 2) is True
