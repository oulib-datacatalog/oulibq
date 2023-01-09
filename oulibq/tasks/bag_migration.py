from celery.task import task
from celery import states
from celery.exceptions import Ignore
from celery.task.sets import TaskSet
#from dockertask import docker_task
from subprocess import call,STDOUT
#import requests
import os, hashlib, bagit,time,sys
from os.path import ismount
from pymongo import MongoClient
import boto3,shutil,requests
from pandas import read_csv
#from cStringIO import StringIO
#Default base directory
#basedir="/data/static/"
from distutils.dir_util import copy_tree
import ConfigParser
import logging

logging.basicConfig(level=logging.INFO)

class NotABag(Exception):
    pass


class BagAlreadyExists(Exception):
    pass


class BagDoesNotExist(Exception):
    pass


def _get_config_parameter(group,param,config_file="cybercom.cfg"):
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    return config.get(group,param)
def _api_get(bag,return_all=None):
    base_url = _get_config_parameter('api','base_url')
    if return_all:
        api_url = "{0}catalog/data/catalog/digital_objects/.json?page_size=0".format(base_url)
    else:
        query= '{"filter":{"bag":"%s"}}' % (bag)
        api_url = "{0}catalog/data/catalog/digital_objects/.json?query={1}".format(base_url,query)
    req =requests.get(api_url)
    if req.status_code > 300:
        raise Exception("Request Error: {0} {1}".format(api_url,req.text))
    return req.json()
def _api_save(data):
    token = _get_config_parameter('api','token')
    base_url = _get_config_parameter('api','base_url')
    headers ={"Content-Type":"application/json","Authorization":"Token {0}".format(token)}
    api_url = "{0}catalog/data/catalog/digital_objects/.json".format(base_url)
    req = requests.post(api_url,data=json.dumps(data),headers=headers)
    req.raise_for_status()
    return True


def get_celery_worker_config(api_host=None):
    #check if environ vars are available
    if not os.getenv('REMOTE_BAGIT_SRC_PATH', None) or not os.getenv('LOCAL_BAGIT_SRC_PATH', None) or not os.getenv('REMOTE_BAGIT_DEST_PATH', None):
        raise Exception("Environmental Variables not set!")
    #set config variables
    config ={"s3":{"bucket": "ul-bagit"},
            "nas":{"bagit":os.getenv('REMOTE_BAGIT_SRC_PATH', None) ,"bagit2": os.getenv('LOCAL_BAGIT_SRC_PATH', None) },
            "norfile":{"bagit": os.getenv('REMOTE_BAGIT_DEST_PATH', None)}}
    return config


def on_mounted_filesystem(path):
    """ check that path is on a mounted filesystem """
    stack = []
    for item in path.split("/"):
        stack.append(item)
        if ismount("/".join(stack)):
            return True
    return False


@task(bind=True)
def copy_bag(self,bag_name,source_path,dest_path):
    """
        This task copies bag from NAS to Norfile. Task must have access to source and destination.

        args:
            bag_name -  string bag name
            source_path - string source path to NAS location. Do not include bag name in variable.
            dest_path - string destination path to Norfile location. Do not include bag name in variable.
    """
    baglist=bag_name.split('/')
    if len(baglist)>1:
        dest=os.path.join(dest_path,baglist[0])
    else:
        dest=dest_path
    if not on_mounted_filesystem(dest):
        msg="The destination is not on a mounted filesystem: {0}".format(dest)
        raise Exception(msg)
    source = os.path.join(source_path,bag_name)
    if not os.path.isdir(source):
        msg="Bag source directory does not exist. {0}".format(source)
        raise Exception(msg)
        #self.update_state(state=states.FAILURE,meta=msg)
        #raise Ignore()
    task_id = str(copy_bag.request.id)
    log=open("{0}.tmp".format(task_id),"w+")
    #status=call(['rsync','-rltD','--delete',source,dest],stderr=log)
    status=call(['sudo','rsync','-rltD',source,dest],stderr=log)
    if status != 0:
        log.seek(0)
        msg= log.read()
        log.close()
        self.update_state(state=states.FAILURE,meta=msg)
        raise Ignore()
    
    msg="Bag copied from {0} to {1}".format(source,dest)
    log.close()
    log.close()
    return msg

@task(bind=True)
def upload_bag_s3(self,bag_name,source_path,s3_bucket,s3_location):
    """
        This task uploads Norfile bag to AWS S3 bucket.

        args:
            bag_name (string): Bag Name.
            source_path (string): Path to Norfile location. Do not include bag name in path.
            s3_bucket (string): S3 bucket
            s3_location (string): key within bucket. Example - 'source/Baldi_1706'
    """
    source ="{0}/{1}".format(source_path,bag_name)
    if not os.path.isdir(source):
        msg="Bag source directory does not exist. {0}".format(source)
        raise Exception(msg)
    s3_loc = "s3://{0}/{1}".format(s3_bucket,s3_location)
    task_id = str(upload_bag_s3.request.id)
    log=open("{0}.tmp".format(task_id),"w+")
    bin_path = os.path.split(os.path.abspath(sys.executable))[0]
    #status=call(['{0}/aws'.format(bin_path),'s3','sync','--delete', source,s3_loc],stderr=log)
    status=call(['{0}/aws'.format(bin_path),'s3','sync', source,s3_loc],stderr=log)
    if status != 0:
        log.seek(0)
        msg= log.read()
        log.close()
        os.remove("{0}.tmp".format(task_id))
        self.update_state(state=states.FAILURE,meta=msg)
        raise Ignore()
    else:
        msg="Bag uploaded from {0} to {1}".format(source,s3_loc)
        log.close()
        os.remove("{0}.tmp".format(task_id))
    return msg


@task(bind=True)
def move_bag_nas(self, source_path, destination_path):
    """
    Move / Rename a bag accessible on the DigiLab NAS

    args:
        source_path - current location of bag
        destination_path - location to move bag
    """

    if not os.path.exists("{0}/bagit.txt".format(source_path)):
        logging.error("Attempted to move non-bag: {0}".format(source_path))
        raise NotABag("The source {0} is not a bag!".format(source_path))

    if os.path.exists(destination_path):
        logging.error("Attempted to move bag to existing location: {0}".format(destination_path))
        raise BagAlreadyExists("The destination already exists! Try using a different destination")

    task_id = str(self.request.id)
    log_path = "{0}.tmp".format(task_id)
    log = open(log_path, "w+")
    status = call(['rsync', '-rltD', source_path, destination_path], stderr=log)
    if status != 0:
        log.seek(0)
        msg = log.read()
        log.close()
        self.update_state(state=states.FAILURE, meta=msg)
        raise Ignore()
    if os.path.exists(log_path):
        os.remove(log_path)
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

    s3_bucket = get_celery_worker_config()['s3']['bucket']
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    try:
        s3.head_object(Bucket=s3_bucket, Key="{0}/bagit.txt".format(source_path))
    except:
        logging.error("Attempted to move non-bag: {0}".format(source_path))
        raise NotABag("The source {0} is not a bag!".format(source_path))

    try:
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=destination_path):
            page["Contents"]
            # Do not allow writing to an existing destination!
            logging.error("Attempted to move bag to existing location: {0}".format(destination_path))
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
    data = _api_get(bag_name)
    metadata = data['results'][0]
    updated_verfied = [item.replace(source_path, destination_path) for item in metadata["locations"]["s3"]["verified"]]
    metadata["locations"]["s3"]["verified"] = updated_verfied
    _api_save(metadata)

    logging.info("Moved S3 objects from {0} to {1}".format(source_path, destination_path))