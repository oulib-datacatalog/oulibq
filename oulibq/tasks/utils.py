import hashlib
import logging
import os
import re
import time
from copy import deepcopy
from functools import partial

import bagit
from bson.objectid import ObjectId
from celery import Celery
try:
    import celeryconfig
except ImportError:
    celeryconfig = None

from six import PY2, PY3

if PY3:
    from pathlib import Path

from .config import INVENTORY_METADATA, BAG_LOCATIONS, PRIVATE_LOCATIONS
from .config import DEFAULT_DAYS_TO_WAIT

app = Celery()
app.config_from_object(celeryconfig)

logging.basicConfig(level=logging.INFO)


class BagNotFoundError(BaseException):
    pass


def query_metadata(search, **kwargs):
    """ query digital objects with search parameter returning iterator of all matches
        kwargs allows filtering returned fields. ex: bag=1, _id=0 would limit returned fields to just bag
        refer to mongodb documentation: https://docs.mongodb.com/manual/tutorial/query-documents/
    """
    db_client = app.backend.database.client
    digital_objects = db_client.catalog.digital_objects
    for result in digital_objects.find(search, kwargs):
        yield result


def get_metadata(bagname):
    """ query digital object by bag name returning metadata of first matching item """
    db_client = app.backend.database.client
    digital_objects = db_client.catalog.digital_objects
    result = digital_objects.find({'bag': bagname}).limit(1)
    if result.count():
        return result[0]
    schema = deepcopy(INVENTORY_METADATA)
    schema['bag'] = bagname
    return schema


def update_metadata(record):
    """ update digital object metadata """
    db_client = app.backend.database.client
    digital_objects = db_client.catalog.digital_objects
    result = digital_objects.update_one({'_id': ObjectId(record['_id'])}, record)
    return result['updatedExisting']


def update_metadata_subfield(record):
    """ update a subfield of a digital object record
        refer to MongoDB's documentation for more details:
        https://docs.mongodb.com/manual/reference/operator/update/set/
    """
    db_client = app.backend.database.client
    digital_objects = db_client.catalog.digital_objects
    result = digital_objects.update_one({'_id': ObjectId(record['_id'])}, {"$set": record})
    return result['updatedExisting']


def upsert_metadata(record):
    """ update existing or insert new digital object metadata
        record must have a matching '_id' to update
        otherwise, the record is inserted as a new record
    """
    db_client = app.backend.database.client
    digital_objects = db_client.catalog.digital_objects
    if record.get('_id'):
        result = digital_objects.update_one({'_id': ObjectId(record['_id'])}, record)
        return result['updatedExisting']
    else:
        result = digital_objects.insert_one(record)
        return result.acknowledged


def is_tombstone(path):
    """ check if a "tombstone" file exists at path """
    return os.path.isfile(path)


def touch_file(path):
    """ touches the file at path, creating it or updating its modified timestamp """
    try:
        if PY3:
            Path(path).touch()
            return True
        else:
            with open(path, 'a') as f:
                f.close()
            return True
    except PermissionError as e:
        logging.error(e)
    return False


def is_bag_valid(path):
    """ check path for valid bag, returning boolean """
    try:
        bag = bagit.Bag(path)
        if bag.has_oxum():
            return bag.is_valid(fast=True)
        else:
            return bag.validate(processes=4)
    except bagit.BagError as e:
        print(e)
        return False


def is_older_than(path, days=DEFAULT_DAYS_TO_WAIT):
    """ check modified time of path is older than specified days, returning boolean """
    seconds = days * 24 * 60 * 60
    return (time.time() - os.stat(path).st_mtime) >= seconds


def list_older(items, days=DEFAULT_DAYS_TO_WAIT):
    """ check list of paths returning a list of those with a modified time older than specified days """
    items = items if type(items) is list else [items]
    directories = [item for item in items if os.path.isdir(item)]
    older_map = map(partial(is_older_than, days=days), directories)
    older_bags = [directories[i] for i, x in enumerate(older_map) if x is True]
    return older_bags


def _filter_bags(items, valid=True, limit_older=True, days=DEFAULT_DAYS_TO_WAIT):
    """ filter list of bagged items by validity and age """
    items = items if type(items) is list else [items]
    if limit_older:
        directories = list_older(items, days=days)
    else:
        directories = [item for item in items if os.path.isdir(item)]
    valid_map = map(is_bag_valid, directories)
    filtered_bags = [directories[i] for i, x in enumerate(valid_map) if x is valid]
    return filtered_bags


def list_invalid(bags, limit_older=True, days=DEFAULT_DAYS_TO_WAIT):
    """ filter list of bags returning list of invalid bags """
    return _filter_bags(bags, valid=False, limit_older=limit_older, days=days)


def list_valid(bags, limit_older=True, days=DEFAULT_DAYS_TO_WAIT):
    """ filter list of bags returning list of valid bags """
    return _filter_bags(bags, valid=True, limit_older=limit_older, days=days)


def iterate_bags(paths=BAG_LOCATIONS['nas']):
    if isinstance(paths, str):
        paths = [paths]
    if isinstance(paths, dict):
        if PY2:
            paths = paths.values()
        else:
            paths = list(paths.values())
    for path in paths:
        for directory in os.listdir(path):
            if directory not in PRIVATE_LOCATIONS:
                yield os.path.join(path, directory)
        for location in PRIVATE_LOCATIONS:
            for directory in os.listdir(os.path.join(path, location)):
                yield os.path.join(path, location, directory)


def is_private(bag):
    """ returns True if bag is in a private locations """
    return any(map(bag.startswith, PRIVATE_LOCATIONS))


def find_bag(bag):
    """
    Function returns path to nas and norfile on current worker. Determines if bag is on Nas1 or Nas2.
    returns NAS Path , Norfile Path, S3 Bucket,S3 Key, S3 folder
    """
    for location in BAG_LOCATIONS["nas"].values():
        if not location:
            continue
        nas_path = os.path.join(location, bag)
        if os.path.isdir(nas_path):
            break
    else:
        raise BagNotFoundError("Checked NAS locations and unable to find bag:{0}.".format(bag))

    s3_folder = "private" if is_private(bag) else "source"
    return nas_path, BAG_LOCATIONS["norfile"]["bagit"], BAG_LOCATIONS["s3"]["bucket"], os.path.join(s3_folder, bag), s3_folder


def mmsid_exists(bag_path):
    """ guess if mmsid exists and return boolean """
    # The MMS ID can be 8 to 19 digits long (with the first two digits referring to the record type and
    # the last four digits referring to a unique identifier for the institution)
    # get an mmsid like value that is not at the beginning of the string
    if re.findall("(?<!^)(?<!\d)\d{8,19}(?!\d)", bag_path):
        return True
    try:
        # check bag-info.txt for an mmsid
        bag = bagit.Bag(bag_path)
        mmsid = bag.info['FIELD_EXTERNAL_DESCRIPTION'].split()[-1].strip()
        if re.match("^[0-9]+$", mmsid):
            return True
    except:
        return False


def list_no_mmsid(items):
    """ Checks multiple bags for mmsid returning list of bags with no mmsid """
    directories = [item for item in items if os.path.isdir(item)]
    exists_map = map(mmsid_exists, directories)
    no_mmsid_bags = [directories[i] for i, x in enumerate(exists_map) if x is False]
    return no_mmsid_bags


def calculate_multipart_etag(source_path, etag, chunk_size=8):
    """
    calculates a multipart upload etag for amazon s3
    Arguments:
        source_path -- The file to calculate the etag
        etag -- s3 etag to compare
    Keyword Arguments:
        chunk_size -- The chunk size to calculate for. Default 8
    """
    md5s = []
    etag_parts = etag.split("-")
    num_parts = etag_parts[1] if len(etag_parts) > 1 else None
    chunk_size = chunk_size * 1024 * 1024
    with open(source_path, 'rb') as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))
    
    if not md5s:
        return False

    # if hash of first chunk matches etag and
    # etag does not list number of chunks
    if md5s[0].hexdigest() == etag and not num_parts:
        return True

    digests = b"".join(m.digest() for m in md5s)
    new_md5 = hashlib.md5(digests)
    new_etag = '%s-%s' % (new_md5.hexdigest(), len(md5s))
    # print source_path,new_etag,etag
    if etag == new_etag:
        return True
    else:
        return False


def sanitize_path(path):
    """ normalizes a path, for example "/data/../etc normalizes to /etc """
    return os.path.normpath(path)
