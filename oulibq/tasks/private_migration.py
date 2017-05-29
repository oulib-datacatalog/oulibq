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
from bag_migration import upload_bag_s3, copy_bag
#Default base directory
#basedir="/data/static/"
from bag_migration import get_celery_worker_config

def _get_bags(path,folder):
    #s3_folder="private/private"
    pvlocation=os.path.join(path,folder)
    return [name for name in os.listdir(pvlocation) if os.path.isdir(os.path.join(pvlocation, name,'data'))]

def _get_bags1(path,folder):
    #s3_folder="private/private"
    pvlocation=os.path.join(path,folder)
    return [os.path.join(folder,name) for name in os.listdir(pvlocation) if os.path.isdir(os.path.join(pvlocation, name,'data'))]

def _gen_subtask_bags(bags,source_path,s3_bucket,s3_folder,celery_queue):
    subtasks=[]
    bag_names=[]
    s3 = boto3.client('s3')
    for bag in bags:
        #double check to make sure not already in s3
        s3_location = os.path.join(s3_folder,bag)
        s3_location = s3_location.replace("//","/")
        source_path = os.path.join(source_path,s3_location)
        s3_key = s3.list_objects(Bucket=s3_bucket, Prefix=s3_location ,MaxKeys=1)
        if not 'Contents' in s3_key:
            subtasks.append(upload_bag_s3.subtask(args=(bag,source_path,s3_bucket,s3_location),queue=celery_queue))
            bag_names.append(s3_location)
        else:
            norfileCount = sum([len(files) for r, d, files in os.walk(os.path.join(source_path,bag))])
            s3_check = s3.list_objects(Bucket=s3_bucket, Prefix=s3_location)
            if len(s3_check['Contents']) != norfileCount:
                subtasks.append(upload_bag_s3.subtask(args=(bag,source_path,s3_bucket,s3_location),queue=celery_queue))
                bag_names.append(s3_location)
    return subtasks,bag_names

@task()
def private_bags_migrate_s3(s3_bucket='ul-bagit',celery_queue="digilab-nas2-prod-workerq",bags=None):
    """
        This task is used at the OU libraries for the migration of bags from Norfile(OU S2) to AWS S3.
        kwargs:
            s3_bucket='ul-bagit'
            s3_folder='private'
            celery_queue='digilab-nas2-prod-workerq'

        This will migrate all bags that have not been uploaded to S3. The task does not care whether or
        not the bag is valid. I have split that task out and will verify bags after replication. If a bag does
        not validata will step back and reload back for Norfile. If Norfile does not validate will migrate back
        to NAS. This provides a consistent upload and migration. This gaurentees backup and will verify later task.

    """
    #Celery worker Config from Catalog
    celery_config=get_celery_worker_config("not used")
    #Norfile bag location
    norfile_bagit=celery_config['norfile']['bagit']
    #All Private Bag Folders with in Norfile
    #Private
    s3_folder="private"
    pvbags1 = _get_bags(norfile_bagit,s3_folder)
    #preservation
    subtasks1,bag_names1=_gen_subtask_bags(pvbags1,norfile_bagit,s3_bucket,os.path.join("private",s3_folder),celery_queue)
    s3_folder="preservation"
    pvbags2 = _get_bags(norfile_bagit,s3_folder)
    subtasks2,bag_names2=_gen_subtask_bags(pvbags2,norfile_bagit,s3_bucket,os.path.join("private",s3_folder),celery_queue)
    #shareok
    s3_folder="shareok"
    pvbags3 = _get_bags(norfile_bagit,s3_folder)
    subtasks3,bag_names3=_gen_subtask_bags(pvbags3,norfile_bagit,s3_bucket,os.path.join("private",s3_folder),celery_queue)
    # Combine the 3 locations
    subtasks = subtasks1 + subtasks2+ subtasks3
    bag_names = bag_names1 + bag_names2 + bag_names3
    if subtasks:
        job = TaskSet(tasks=subtasks)
        result_set = job.apply_async()

    names=",".join(bag_names)
    return "{0} subtasks('upload_bag_s3') submitted. Bags: {1}".format(len(subtasks),names)

@task()
def private_bags_migrate_norfile(olderThanDays=3,celery_queue="digilab-nas2-prod-workerq"):
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
    nas_bagit= celery_config['nas']['bagit']
    #get bags
    nas_folder="private"
    pvbags1 = _get_bags1(nas_bagit,nas_folder)
    nas_folder = "preservation"
    pvbags2 = _get_bags1(nas_bagit,nas_folder)
    nas_folder = "shareok"
    pvbags3 = _get_bags1(nas_bagit,nas_folder)

    bags= pvbags1 + pvbags2 + pvbags3
    #Time Variables
    olderThanDays *= 86400 # convert days to seconds
    present = time.time()
    subtasks=[]
    bag_names=[]
    for bag in bags:
        if  (present - os.path.getmtime(os.path.join(nas_bagit, bag))) > olderThanDays:
            if not os.path.isdir(os.path.join(norfile_bagit,bag)):
                subtasks.append(copy_bag.subtask(args=(bag,nas_bagit,norfile_bagit),queue=celery_queue))
                bag_names.append(bag)
    if subtasks:
        job = TaskSet(tasks=subtasks)
        result_set = job.apply_async()

    names=",".join(bag_names)
    return "{0} subtasks('copy_bag') submitted. Bags: {1}".format(len(subtasks),names)
