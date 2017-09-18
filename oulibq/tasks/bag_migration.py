from celery.task import task
from celery import states
from celery.exceptions import Ignore
from celery.task.sets import TaskSet
#from dockertask import docker_task
from subprocess import call,STDOUT
#import requests
import os, hashlib, bagit,time,sys
from pymongo import MongoClient
import boto3,shutil,requests
from pandas import read_csv
#from cStringIO import StringIO
#Default base directory
#basedir="/data/static/"
from distutils.dir_util import copy_tree

def get_celery_worker_config(api_host):
    #check if environ vars are available
    if not os.getenv('REMOTE_BAGIT_SRC_PATH', None) or not os.getenv('LOCAL_BAGIT_SRC_PATH', None) or not os.getenv('REMOTE_BAGIT_DEST_PATH', None):
        raise Exception("Environmental Variables not set!")
    #set config variables
    config ={"s3":{"bucket": "ul-bagit"},
            "nas":{"bagit":os.getenv('REMOTE_BAGIT_SRC_PATH', None) ,"bagit2": os.getenv('LOCAL_BAGIT_SRC_PATH', None) },
            "norfile":{"bagit": os.getenv('REMOTE_BAGIT_DEST_PATH', None)}}
    return config


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
    source = os.path.join(source_path,bag_name)
    if not os.path.isdir(source):
        msg="Bag source directory does not exist. {0}".format(source)
        raise Exception(msg)
        #self.update_state(state=states.FAILURE,meta=msg)
        #raise Ignore()
    task_id = str(copy_bag.request.id)
    log=open("{0}.tmp".format(task_id),"w+")
    #status=call(['rsync','-rltD','--delete',source,dest],stderr=log)
    status=call(['rsync','-rltD',source,dest],stderr=log)
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
