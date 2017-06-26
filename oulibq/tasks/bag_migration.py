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
from cStringIO import StringIO
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

@task()
def bags_migrate_s3(s3_bucket='ul-bagit',s3_folder='source',celery_queue="digilab-nas2-prod-workerq",bags=None):
    """
        This task is used at the OU libraries for the migration of bags from Norfile(OU S2) to AWS S3.
        kwargs:
            s3_bucket='ul-bagit'
            s3_folder='source'
            api_host='dev.libraries.ou.edu'

        This will migrate all bags that have not been uploaded to S3. The task does not care whether or
        not the bag is valid. I have split that task out and will verify bags after replication. If a bag does
        not validata will step back and reload back for Norfile. If Norfile does not validate will migrate back
        to NAS. This provides a consistent upload and migration. This gaurentees backup and will verify later task.

    """
    #Celery worker Config from Catalog
    celery_config=get_celery_worker_config("not used")
    #Norfile bag location
    norfile_bagit=celery_config['norfile']['bagit']
    #All Bag Folders with in Norfile
    if bags:
        bags = bags.split(',')
    else:
        bags =[name for name in os.listdir(norfile_bagit) if os.path.isdir(os.path.join(norfile_bagit, name,'data'))]
    #s3_bucket=celery_config['s3']['bucket']
    subtasks=[]
    bag_names=[]
    s3 = boto3.client('s3')
    for bag in bags:
        #double check to make sure not already in s3
        s3_location = "{0}/{1}".format(s3_folder,bag)
        s3_location = s3_location.replace("//","/")
        s3_key = s3.list_objects(Bucket=s3_bucket, Prefix=s3_location ,MaxKeys=1)
        if not 'Contents' in s3_key:
            subtasks.append(upload_bag_s3.subtask(args=(bag,norfile_bagit,s3_bucket,s3_location),queue=celery_queue))
            bag_names.append(s3_location)
        else:
            norfileCount = sum([len(files) for r, d, files in os.walk('{0}/{1}'.format(norfile_bagit,bag))])
            s3_check = s3.list_objects(Bucket=s3_bucket, Prefix=s3_location)
            if len(s3_check['Contents']) != norfileCount:
                subtasks.append(upload_bag_s3.subtask(args=(bag,norfile_bagit,s3_bucket,s3_location),queue=celery_queue))
                bag_names.append(s3_location)
    if subtasks:
        job = TaskSet(tasks=subtasks)
        result_set = job.apply_async()

    names=",".join(bag_names)
    return "{0} subtasks('upload_bag_s3') submitted. Bags: {1}".format(len(subtasks),names)

@task()
def bags_migrate_norfile(olderThanDays=3,celery_queue="digilab-nas2-prod-workerq"):
    """
        This task is used at the OU libraries for the migration of bags from Digilab NAS to Norfile(OU S2).
        kwargs:
            olderThanDays= Default 3
            api_host= Default dev.libraries.ou.edu

        This will migrate all bags from DigiLab NAS to Norfile. The task does not care whether or not the bag
        is valid. I have split that task out and will verify bags after replication. If a bag does not validate
        process will step back and reload back from NAS Location. This provides a consistent upload and migration.
        This guarantee backup and will run verification task at a later time.
    """

    #Celery worker Config from Catalog
    celery_config=get_celery_worker_config("not used")
    #Bag locations
    norfile_bagit=celery_config['norfile']['bagit']
    nas_bagit= celery_config['nas']['bagit2']

    bags=[name for name in os.listdir(nas_bagit) if os.path.isdir(os.path.join(nas_bagit, name,'data'))]
    #Time Variables
    olderThanDays *= 86400 # convert days to seconds
    present = time.time()
    subtasks=[]
    bag_names=[]
    for bag in bags:
        if  (present - os.path.getmtime(os.path.join(nas_bagit, bag))) > olderThanDays:
            if not os.path.isdir("{0}/{1}".format(norfile_bagit,bag)):
                subtasks.append(copy_bag.subtask(args=(bag,nas_bagit,norfile_bagit),queue=celery_queue))
                bag_names.append(bag)
    if subtasks:
        job = TaskSet(tasks=subtasks)
        result_set = job.apply_async()

    names=",".join(bag_names)
    return "{0} subtasks('copy_bag') submitted. Bags: {1}".format(len(subtasks),names)

@task(bind=True)
def copy_bag(self,bag_name,source_path,dest_path):
    """
        This task copies bag from NAS to Norfile. Task must have access to source and destination.

        args:
            bag_name -  string bag name
            source_path - string source path to NAS location. Do not include bag name in variable.
            dest_path - string destination path to Norfile location. Do not include bag name in variable.
    """
    dest="{0}/{1}".format(dest_path,bag_name)
    source = "{0}/{1}".format(source_path,bag_name)
    if not os.path.isdir(source):
        msg="Bag source directory does not exist. {0}".format(source)
        self.update_state(state=states.FAILURE,meta=msg)
        raise Ignore()
    log=StringIO()
    status=call(['rsync','-rltD',source,dest],stderr=log)
    if status != 0:
        msg= log.getvalue()
        log.close()
        self.update_state(state=states.FAILURE,meta=msg)
        raise Ignore()
    
    msg="Bag copied from {0} to {1}".format(source,dest)
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
    s3_loc = "s3://{0}/{1}".format(s3_bucket,s3_location)
    task_id = str(upload_bag_s3.request.id)
    log=open("{0}.tmp".format(task_id),"w+")
    bin_path = os.path.split(os.path.abspath(sys.executable))[0]
    status=call(['{0}/aws'.format(bin_path),'s3','sync',source,s3_loc],stderr=log)
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
