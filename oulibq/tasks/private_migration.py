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
from tasks import _api_get,_api_save,validate_nas_files,validate_s3_files,validate_norfile_bag
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
        #source_path = os.path.join(source_path,s3_location)
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
def private_digilab_inventory(project="private",department="DigiLab",force=False,celery_queue="digilab-nas2-prod-workerq"):
    #Celery worker Config from Catalog
    celery_config=get_celery_worker_config("not used")
    #Norfile bag location
    norfile_bagit=celery_config['norfile']['bagit']
    nas_bagit = celery_config['nas']['bagit']
    s3_bucket=celery_config['s3']['bucket']
    #All Private Bag Folders with in Norfile
    #Private
    s3_folder="private"
    pvbags1 = _get_bags1(norfile_bagit,s3_folder)
    #preservation
    s3_folder="preservation"
    pvbags2 = _get_bags1(norfile_bagit,s3_folder)
    #shareok
    s3_folder="shareok"
    pvbags3 = _get_bags1(norfile_bagit,s3_folder)
    subtask=[]
    bags = pvbags1 + pvbags2 + pvbags3
    update_cat=0
    new_cat=0
    subtasks=[]
    for bag in bags:
        data = _api_get(bag)
        if data['count']>0:
            inventory_metadata = data['results'][0]
            update_cat+=1
        else:
            #new item to inventory
            inventory_metadata={ 'derivatives':{},'project':'','department':'', 'bag':bag,'locations':{
                                's3':{'exists':False,'valid':False,'bucket':'','validation_date':'','manifest':'','verified':[],'error':[]},
                                'norfile':{'exists':False,'valid':False,'validation_date':'','location':'UL-BAGIT'},
                                'nas':{'exists':False,'place_holder':False,'location':''}}}
            new_cat+=1
        if project:
            inventory_metadata['project']=project
        if department:
            inventory_metadata['department']=department
        #save inventory metadata
        _api_save(inventory_metadata)
        # norfile validation
        if not inventory_metadata['locations']['norfile']['valid'] or force:
            subtasks.append(validate_norfile_bag.subtask(args=(bag,norfile_bagit),queue=celery_queue))
        #  s3 validataion
        if not inventory_metadata['locations']['s3']['valid'] or force:
            subtasks.append(validate_s3_files.subtask(args=(bag,norfile_bagit,s3_bucket),kwargs={"s3_base_key":"private"},queue=celery_queue))
        # nas validation
        subtasks.append(validate_nas_files.subtask(args=(bag,nas_bagit),queue=celery_queue))
    if subtasks:
        job = TaskSet(tasks=subtasks)
        result_set = job.apply_async()
    return "Bag Inventory: {0} New, {1} Updates. {2} subtasks submitted".format(new_cat,update_cat,len(subtasks))

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
    pvbags1 = _get_bags1(norfile_bagit,s3_folder)
    #preservation
    subtasks1,bag_names1=_gen_subtask_bags(pvbags1,norfile_bagit,s3_bucket,"private",celery_queue)
    s3_folder="preservation"
    pvbags2 = _get_bags1(norfile_bagit,s3_folder)
    subtasks2,bag_names2=_gen_subtask_bags(pvbags2,norfile_bagit,s3_bucket,"private",celery_queue)
    #shareok
    s3_folder="shareok"
    pvbags3 = _get_bags1(norfile_bagit,s3_folder)
    subtasks3,bag_names3=_gen_subtask_bags(pvbags3,norfile_bagit,s3_bucket,"private",celery_queue)
    # Combine the 3 locations
    #return pvbags1 + pvbags2 + pvbags3
    subtasks = subtasks1 + subtasks2 + subtasks3
    bag_names = bag_names1 + bag_names2 + bag_names3
    #return subtasks
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
            data = _api_get(bag)
            if data['count']>0:
                data = data['results'][0]["locations"]["norfile"]["valid"]
            else:
                data = True
            if not os.path.isdir(os.path.join(norfile_bagit,bag)) or not data:
                subtasks.append(copy_bag.subtask(args=(bag,nas_bagit,norfile_bagit),queue=celery_queue))
                bag_names.append(bag)
    if subtasks:
        job = TaskSet(tasks=subtasks)
        result_set = job.apply_async()

    names=",".join(bag_names)
    return "{0} subtasks('copy_bag') submitted. Bags: {1}".format(len(subtasks),names)
