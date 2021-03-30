from celery.task import task
import os, bagit
import boto3, shutil
from botocore.exceptions import ClientError
from pandas import read_csv

from .utils import get_metadata, upsert_metadata, is_tombstone, is_bag_valid, calculate_multipart_etag, query_metadata
from .utils import touch_file, sanitize_path

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
    else:
        nas_metadata['exists'] = True
        nas_metadata['place_holder'] = False
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
    s3_key = s3.list_objects(Bucket=s3_bucket, Prefix="{0}/{1}".format(s3_base_key, bag_name).decode('utf-8'),
                             MaxKeys=1)
    if 'Contents' in s3_key:
        s3_metadata['exists'] = True
        manifest = "{0}/{1}/manifest-md5.txt".format(local_source_path, bag_name).decode('utf-8')
        s3_metadata['manifest'] = manifest
        # Read manifest
        data = read_csv(manifest, sep="  ", names=['md5', 'filename'], header=None, )
        s3_metadata['bucket'] = s3_bucket
        s3_metadata['verified'] = []
        s3_metadata['error'] = []
        s3_metadata['valid'] = False
        for index, row in data.iterrows():
            bucket_key = "{0}/{1}/{2}".format(s3_base_key, bag_name, row.filename).decode('utf-8')
            local_bucket_key = "{0}/{1}".format(bag_name, row.filename).decode('utf-8')
            try:
                etag = s3.head_object(Bucket=s3_bucket, Key=bucket_key)['ETag'][1:-1]
            except ClientError:
                errormsg = u"Failed to get S3 object header for key: {0}".format(bucket_key)
                logging.error(errormsg)
                raise Exception(errormsg)
            if calculate_multipart_etag(u"{0}/{1}".format(local_source_path, local_bucket_key),
                                        etag) or etag == row.md5:
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
    return "Bags removed: {0}, Bags removal Errors: {1} Bags with Errors:{2} ".format(removed, len(errors), bag_errors)


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
        msg = "Bag source directory does not exist. {0}".format(source)
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

    msg = "Bag copied from {0} to {1}".format(source, dest)
    log.close()
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
    source = "{0}/{1}".format(source_path, bag_name)
    if not os.path.isdir(source):
        msg = "Bag source directory does not exist. {0}".format(source)
        raise Exception(msg)
    s3_loc = "s3://{0}/{1}".format(s3_bucket, s3_location)
    task_id = str(upload_bag_s3.request.id)
    log = open("{0}.tmp".format(task_id), "w+")
    bin_path = os.path.split(os.path.abspath(sys.executable))[0]
    # status=call(['{0}/aws'.format(bin_path),'s3','sync','--delete', source,s3_loc],stderr=log)
    status = call(['{0}/aws'.format(bin_path), 's3', 'sync', source, s3_loc], stderr=log)
    if status != 0:
        log.seek(0)
        msg = log.read()
        log.close()
        os.remove("{0}.tmp".format(task_id))
        self.update_state(state=states.FAILURE, meta=msg)
        raise Ignore()
    else:
        msg = "Bag uploaded from {0} to {1}".format(source, s3_loc)
        log.close()
        os.remove("{0}.tmp".format(task_id))
    return msg


@task()
def remove_nas_files(bag_name):
    """
    Remove NAS bag
    agrs:
        bag_name
    """
    inventory_metadata = get_metadata(bag_name)
    nas_metadata = inventory_metadata['locations']['nas']
    bag_location = nas_metadata['location']
    if (not bag_location == "/") and (len(bag_location) > 15) and (sanitize_path(bag_location) == bag_location):
        if not nas_metadata['exists']:
            raise Exception("Bag: {0} has already been removed".format(bag_name))
        try:
            shutil.rmtree(bag_location)
            nas_metadata['exists'] = False
            # create placeholder / tombstone
            touch_file(bag_location)
            nas_metadata['place_holder'] = True
        except Exception as e:
            msg = "Error removing files: {0}. {1}".format(bag_location, str(e))
            logging.error(msg)
            nas_metadata['error'] = msg
        finally:
            upsert_metadata(inventory_metadata)
    else:
        logging.error("Suspicious Bag location: Security Error - {0}".format(bag_location))
        raise Exception("Suspicious Bag location: Security Error - {0}".format(bag_location))