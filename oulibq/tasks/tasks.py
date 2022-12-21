import os
import shutil
import sys
import boto3

from six import ensure_text
from subprocess import call
from botocore.exceptions import ClientError
from celery import states
from celery.exceptions import Ignore

from celery import Celery
app = Celery()
task = app.task

from .utils import get_metadata, upsert_metadata, is_tombstone, is_bag_valid, calculate_multipart_etag, query_metadata
from .utils import touch_file, sanitize_path, update_metadata_subfield
from .config import BAG_LOCATIONS
from .config import NotFullyReplicated, SuspiciousLocation, BagDoesNotExist, BagAlreadyExists, NotABag


import logging

logging.basicConfig(level=logging.INFO)


@task()
def validate_nas_files(bag_name, local_source_paths):
    """
    Validation of NAS

    Agrs:
        bag_name - string
        local_source_paths - list of sources
    """
    inventory_metadata = get_metadata(bag_name)
    nas_metadata = inventory_metadata['locations']['nas']
    local_source_path = os.path.join(local_source_paths, bag_name)
    nas_metadata['location'] = local_source_path
    nas_metadata['error'] = ""
    if not os.path.exists(local_source_path):
        nas_metadata['error'] = "Bag Folder or placeholder not found."
    elif is_tombstone(local_source_path):
        nas_metadata['exists'] = False
        nas_metadata['place_holder'] = True
    elif is_bag_valid(local_source_path):
        nas_metadata['exists'] = True
        nas_metadata['place_holder'] = False
    else:
        nas_metadata['error'] = "Failed bag validation."
    upsert_metadata(inventory_metadata)
    return {'status': "SUCCESS", 'args': [bag_name, local_source_paths], 'nas': inventory_metadata['locations']['nas']}


@task()
def validate_s3_files(bag_name, local_source_path, s3_bucket, s3_base_key='source'):
    """
    Validate s3 files
    args:
        bag_name,local_source_path,s3_bucket
    kwargs:
        s3_base_key='source'
    """
    # Find metadata
    inventory_metadata = get_metadata(bag_name)
    s3_metadata = inventory_metadata['locations']['s3']
    # S3 bucket
    s3 = boto3.client('s3')
    s3_key = s3.list_objects(Bucket=s3_bucket, Prefix=ensure_text("{0}/{1}").format(s3_base_key, bag_name), MaxKeys=1)
    if 'Contents' in s3_key:
        s3_metadata['exists'] = True
        manifest = ensure_text("{0}/{1}/manifest-md5.txt").format(local_source_path, bag_name)
        s3_metadata['manifest'] = manifest
        # Read manifest
        manifest_items = []
        with open(manifest, "r") as f:
            for line in f.readlines():
                # filenames can contain multiple spaces, splitting hash and
                # filename by the first occurence of doublespace
                # hash_val, *filename = line.split("  ")  # This only works in 3.x+
                line_split = iter(line.split("  "))
                hash_val, filename = next(line_split), list(line_split)
                filename = "  ".join(filename).strip()
                manifest_items.append({
                    "md5": hash_val,
                    "filename": filename
                    })
        s3_metadata['bucket'] = s3_bucket
        s3_metadata['verified'] = []
        s3_metadata['error'] = []
        s3_metadata['valid'] = False
        for row in manifest_items:
            md5, filename = row['md5'], row['filename']
            bucket_key = ensure_text("{0}/{1}/{2}").format(s3_base_key, bag_name, filename)
            local_bucket_key = ensure_text("{0}/{1}").format(bag_name, filename)
            try:
                etag = s3.head_object(Bucket=s3_bucket, Key=bucket_key)['ETag'][1:-1]
            except ClientError:
                errormsg = ensure_text("Failed to get S3 object header for key: {0}").format(bucket_key)
                logging.error(errormsg)
                raise Exception(errormsg)
            if calculate_multipart_etag(ensure_text("{0}/{1}").format(local_source_path, local_bucket_key),
                                        etag) or etag == md5:
                s3_metadata['verified'].append(bucket_key)
            else:
                s3_metadata['error'].append(bucket_key)
        if len(s3_metadata['error']) == 0:
            s3_metadata['valid'] = True
    else:
        s3_metadata['exists'] = False
    upsert_metadata(inventory_metadata)
    return {'status': "SUCCESS", 'args': [bag_name, local_source_path, s3_bucket], 's3': s3_metadata}


@task()
def validate_norfile_bag(bag_name, local_source_path):
    """
    Validate Norfile bag
    args:
        bag_name,local_source_path
    """
    inventory_metadata = get_metadata(bag_name)
    norfile_metadata = inventory_metadata['locations']['norfile']
    bag_path = os.path.join(local_source_path, bag_name)
    if os.path.isdir(bag_path):
        norfile_metadata['exists'] = True
        norfile_metadata['valid'] = is_bag_valid(bag_path)
    else:
        norfile_metadata['exists'] = False
        norfile_metadata['valid'] = False
    upsert_metadata(inventory_metadata)
    return {'status': "SUCCESS", 'args': [bag_name, local_source_path], 'norfile': norfile_metadata}


@task()
def clean_nas_files():
    """
    Clean NAS files is a task that checks the bagit inventory catalog for bags that can be deleted from NAS

    """
    errors = []
    removed = 0
    query = {
        "locations.s3.valid": True,
        "locations.norfile.valid": True,
        "locations.nas.exists": True
    }
    for record in query_metadata(query, bag=1, _id=0):  # return the "bag" field only
        try:
            remove_nas_files(record['bag'])
            removed += 1
        except Exception as e:
            errors.append(str(e))
    bag_errors = ", ".join(errors)
    return ensure_text("Bags removed: {0}, Bags removal Errors: {1} Bags with Errors:{2} ").format(removed, len(errors), bag_errors)


@task(bind=True)
def copy_bag(self, bag_name, source_path, dest_path):
    """
        This task copies bag from NAS to Norfile. Task must have access to source and destination.

        args:
            bag_name -  string bag name
            source_path - string source path to NAS location. Do not include bag name in variable.
            dest_path - string destination path to Norfile location. Do not include bag name in variable.
    """
    baglist = bag_name.split('/')
    if len(baglist) > 1:
        dest = os.path.join(dest_path, baglist[0])
    else:
        dest = dest_path
    source = os.path.join(source_path, bag_name)
    if not os.path.isdir(source):
        msg = ensure_text("Bag source directory does not exist. {0}").format(source)
        raise Exception(msg)
        # self.update_state(state=states.FAILURE,meta=msg)
        # raise Ignore()
    task_id = str(copy_bag.request.id)
    log = open("{0}.tmp".format(task_id), "w+")
    # status=call(['rsync','-rltD','--delete',source,dest],stderr=log)
    status = call(['sudo', 'rsync', '-rltD', source, dest], stderr=log)
    if status != 0:
        log.seek(0)
        msg = log.read()
        log.close()
        self.update_state(state=states.FAILURE, meta=msg)
        raise Ignore()

    msg = ensure_text("Bag copied from {0} to {1}").format(source, dest)
    log.close()
    return msg


@task(bind=True)
def upload_bag_s3(self, bag_name, source_path, s3_bucket, s3_location):
    """
        This task uploads Norfile bag to AWS S3 bucket.

        args:
            bag_name (string): Bag Name.
            source_path (string): Path to Norfile location. Do not include bag name in path.
            s3_bucket (string): S3 bucket
            s3_location (string): key within bucket. Example - 'source/Baldi_1706'
    """
    source = ensure_text("{0}/{1}").format(source_path, bag_name)
    if not os.path.isdir(source):
        msg = ensure_text("Bag source directory does not exist. {0}").format(source)
        raise Exception(msg)
    s3_loc = ensure_text("s3://{0}/{1}").format(s3_bucket, s3_location)
    task_id = str(upload_bag_s3.request.id)
    log = open(ensure_text("{0}.tmp").format(task_id), "w+")
    bin_path = os.path.split(os.path.abspath(sys.executable))[0]
    # status=call(['{0}/aws'.format(bin_path),'s3','sync','--delete', source,s3_loc],stderr=log)
    status = call(['{0}/aws'.format(bin_path), 's3', 'sync', source, s3_loc], stderr=log)
    if status != 0:
        log.seek(0)
        msg = log.read()
        log.close()
        os.remove(ensure_text("{0}.tmp").format(task_id))
        self.update_state(state=states.FAILURE, meta=msg)
        raise Ignore()
    else:
        msg = ensure_text("Bag uploaded from {0} to {1}").format(source, s3_loc)
        log.close()
        os.remove(ensure_text("{0}.tmp").format(task_id))
    return msg


@task(bind=True)
def remove_nas_files(self, bag_name):
    """
    Remove NAS bag
    agrs:
        bag_name
    """
    inventory_metadata = get_metadata(bag_name)
    nas_metadata = inventory_metadata['locations']['nas']
    norfile_valid = inventory_metadata['locations']['norfile']['valid']
    s3_valid = inventory_metadata['locations']['s3']['valid']
    bag_location = nas_metadata['location']

    if not norfile_valid or not s3_valid:
        raise NotFullyReplicated(ensure_text("{0} is not fully replicated - not removing from NAS!").format(bag_name))
    elif (not bag_location == "/") and (len(bag_location) > 15) and (sanitize_path(bag_location) == bag_location):
        if not nas_metadata['exists']:
            raise BagDoesNotExist(ensure_text("Bag: {0} has already been removed").format(bag_name))
        try:
            shutil.rmtree(bag_location)
            nas_metadata['exists'] = False
            # create placeholder / tombstone
            touch_file(bag_location)
            nas_metadata['place_holder'] = True
        except Exception as e:
            msg = ensure_text("Error removing files: {0}. {1}").format(bag_location, str(e))
            logging.error(msg)
            nas_metadata['error'] = msg
        finally:
            upsert_metadata(inventory_metadata)    
    else:
        msg = ensure_text("Suspicious Bag location set for removal from task {0}: {1}").format(self.request.id, bag_location)
        logging.error(msg)
        raise SuspiciousLocation(msg)


@task(bind=True)
def move_bag_nas(self, source_path, destination_path):
    """
    Move / Rename a bag accessible on the DigiLab NAS

    args:
        source_path - current location of bag 
        destination_path - location to move bag
    """
    
    if not os.path.exists(ensure_text("{0}/bagit.txt".format(source_path))):
        raise NotABag(ensure_text("The source {0} is not a bag!".format(source_path)))

    if os.path.exists(destination_path):
        raise BagAlreadyExists("The destination already exists! Try using a different destination")

    task_id = str(self.request.id)
    log = open("{0}.tmp".format(task_id), "w+")
    status = call(['rsync', '-rltD', source_path, destination_path], stderr=log)
    if status != 0:
        log.seek(0)
        msg = log.read()
        log.close()
        os.remove(ensure_text("{0}.tmp").format(task_id))
        self.update_state(state=states.FAILURE, meta=msg)
        raise Ignore()
    shutil.rmtree(source_path)
    logging.info("Moved NAS files from {0} to {1}".format(source_path, destination_path))


@task()
def move_bag_s3(source_path, destination_path):
    """
    Move / Rename a bag accessible in the default S3 bucket

    args:
        source_path - current location of bag 
        destination_path - location to move bag
    """
    
    s3_bucket = BAG_LOCATIONS["s3"]["bucket"]
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    try:
        s3.head_object(Bucket=s3_bucket, Key=ensure_text("{0}/bagit.txt".format(source_path)))
    except:
        raise NotABag(ensure_text("The source {0} is not a bag!".format(source_path)))

    try:
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=destination_path):
            page["Contents"]
            # Do not allow writing to an existing destination!
            raise BagAlreadyExists("The destination already exists! Try using a different destination")
    except KeyError:
        # destination does not exist, allow writing to it
        pass

    for page in paginator.paginate(Bucket=s3_bucket, Prefix=source_path):
        try:
            contents = page["Contents"]
        except KeyError:
            raise BagDoesNotExist("Could not find bag matching request!")
        for item in contents:
            source_key = item["Key"]
            destination_key = source_key.replace(source_path, destination_path)
            copy_source = {"Bucket": s3_bucket, "Key": source_key}
            s3.copy_object(Bucket=s3_bucket, CopySource=copy_source, Key=destination_key)

    # Clean up after successful copy
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=source_path):
        for item in page["Contents"]:
            source_key = item["Key"]
            s3.delete_object(Bucket=s3_bucket, Key=source_key)

    # Update catalog with new s3 location
    bag_name = source_path.split("/")[-1]
    metadata = query_metadata({"bag": bag_name}, {"locations.s3.verified":1})[0]
    updated_verfied = [item.replace(source_path, destination_path) for item in metadata["locations"]["s3"]["verified"]]
    metadata["locations"]["s3"]["verified"] = updated_verfied
    update_metadata_subfield(metadata)

    logging.info("Moved S3 objects from {0} to {1}".format(source_path, destination_path))