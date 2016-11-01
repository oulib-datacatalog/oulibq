from celery.task import task
from celery import states
from celery.exceptions import Ignore
from celery.task.sets import TaskSet
#from dockertask import docker_task
from subprocess import call,STDOUT
#import requests
import os, hashlib, bagit
from pymongo import MongoClient
import boto3,shutil
from pandas import read_csv
#Default base directory 
#basedir="/data/static/"

@task()
def digilab_inventory(bags=None,force=None,project=None,department=None,mongo_host='oulib_mongo'):
    """
    DigiLab Inventory Task
    *REQUIRED*
    1. Catalog must contain Celery worker config. Default celery_worker = "dev-mstacy".
       Default Configuration
        {
            "s3": {
                "bucket": "ul-bagit"
            }, 
            "nas": {
                "bagit": "/mnt/nas/bagit", 
                "bagit2": "/mnt/nas/bagit2"
            }, 
            "celery_worker": "dev-mstacy", 
            "norfile": {
                "bagit": "/mnt/norfile/UL-BAGIT"
            }
        }
    2. AWS CLI client is required and credentials configured!
    *PARAMETERS*
    1. args: None
    2. kwargs:
        bags=None #comma separated string with bag names to inventory.
        force=None #if None then valid components will not be inventory. If not None will re-inventory all bag components
        project= None # add project metadata
        department= None # add project Department information
        mongo_host='oulib_mongo # mongo host connection
    """
    #catalog
    db=MongoClient(mongo_host)
    #Celery Worker storage connections
    celery_worker_hostname = os.getenv('celery_worker_hostname', "dev-mstacy")
    celery_config=db.catalog.celery_worker_config.find_one({"celery_worker":celery_worker_hostname})
    #set variables
    nas_bagit=[celery_config['nas']['bagit2'],celery_config['nas']['bagit']]
    norfile_bagit=celery_config['norfile']['bagit']
    s3_bucket=celery_config['s3']['bucket']
    #get list of bags from norfile
    valid_bags=[]
    if bags:
        valid_bags=bags.split(',')
    else: 
        valid_bags=[name for name in os.listdir(norfile_bagit) if os.path.isdir("{0}/{1}".format(norfile_bagit,name))]
    
    #remove hidden folders
    valid_bags = [x for x in valid_bags if not x.startswith(('_','.'))]

    subtasks=[]
    new_cat=0
    update_cat=0
    for bag in valid_bags:
        if db.catalog.bagit_inventory.find({'bag':bag}).count()>0:
            inventory_metadata = db.catalog.bagit_inventory.find_one({'bag':bag})
            update_cat+=1
        else:
            #new item to inventory
            inventory_metadata={'project':'','department':'', 'bag':bag,'s3_bucket':s3_bucket,
                                's3':{'exists':False,'valid':False,'bucket':'','manifest':'','verified':[],'error':[]},
                                'norfile':{'exists':False,'valid':False,'location':'UL-BAGIT'},
                                'nas':{'exists':False,'place_holder':False,'location':''}}
            new_cat+=1
        if project:
            inventory_metadata['project']=project
        if department:
            inventory_metadata['department']=department
        #save inventory metadata
        db.catalog.bagit_inventory.save(inventory_metadata)
        #norfile
        if not inventory_metadata['norfile']['valid'] or force:
            subtasks.append(validate_norfile_bag.subtask(args=(bag,norfile_bagit,mongo_host)))
        #s3
        if not inventory_metadata['s3']['valid'] or force:
            subtasks.append(validate_s3_files.subtask(args=(bag,norfile_bagit,s3_bucket,mongo_host)))
        #nas
        subtasks.append(validate_nas_files.subtask(args=(bag,nas_bagit,mongo_host)))
    if subtasks:
        job = TaskSet(tasks=subtasks)
        result_set = job.apply_async()
    return "Bag Inventory: {0} New, {1} Updates. {2} subtasks submitted".format(new_cat,update_cat,len(subtasks))

@task()
def bags_migrate_s3(mongo_host='oulib_mongo'):
    #catalog
    db=MongoClient(mongo_host)
    #Celery Worker storage connections
    celery_worker_hostname = os.getenv('celery_worker_hostname', "dev-mstacy")
    celery_config=db.catalog.celery_worker_config.find_one({"celery_worker":celery_worker_hostname})
    #get variable by celory worker
    norfile_bagit=celery_config['norfile']['bagit']
    s3_bucket=celery_config['s3']['bucket']
    subtasks=[]
    check_catalog=[]
    s3 = boto3.client('s3')
    for itm in db.catalog.bagit_inventory.find({"s3.exists":False}):
        #double check to make sure not already in s3
        s3_key = s3.list_objects(Bucket=s3_bucket, Prefix=itm['bag'] ,MaxKeys=1)
        if not 'Contents' in s3_key:
            subtasks.append(upload_bag_s3.subtask(args=(itm['bag'],norfile_bagit)))
        else:
            check_catalog.append(itm['bag'])
    if subtasks:
        job = TaskSet(tasks=subtasks)
        result_set = job.apply_async()

    check=",".join(check_catalog)
    return "{0} subtasks('upload_bag_s3') submitted. Check Catalog: {1}".format(len(subtasks),check)

@task()
def validate_nas_files(bag_name,local_source_paths,mongo_host):
    db=MongoClient(mongo_host)
    inventory_metadata = db.catalog.bagit_inventory.find_one({'bag':bag_name})
    location=0
    for local_source_path in local_source_paths:
        if os.path.isdir('{0}/{1}'.format(local_source_path,bag_name)):
            inventory_metadata['nas']['exists']=True
            inventory_metadata['nas']['place_holder']=False
            inventory_metadata['nas']['location']='{0}/{1}'.format(local_source_path,bag_name)
            location+=1
        elif os.path.exists('{0}/{1}'.format(local_source_path,bag_name)):
            inventory_metadata['nas']['exists']=False
            inventory_metadata['nas']['place_holder']=True
            inventory_metadata['nas']['location']='{0}/{1}'.format(local_source_path,bag_name)
            location+=1
    if location>1:
        inventory_metadata['nas']['error']="Multiple nas locations"
    cas=db.catalog.bagit_inventory.find_one({'bag':bag_name})
    cas['nas']=inventory_metadata['nas']
    db.catalog.bagit_inventory.save(cas)
    return {'status':"SUCCESS",'args':[bag_name,local_source_paths,mongo_host],'nas':inventory_metadata['nas']}

@task()
def validate_s3_files(bag_name,local_source_path,s3_bucket,mongo_host):
    db=MongoClient(mongo_host)
    inventory_metadata = db.catalog.bagit_inventory.find_one({'bag':bag_name})
    metadata=inventory_metadata['s3']
    s3 = boto3.client('s3')
    s3_key = s3.list_objects(Bucket=s3_bucket, Prefix=bag_name,MaxKeys=1)
    if 'Contents' in s3_key:
        metadata['exists']=True
        manifest = "{0}/{1}/manifest-md5.txt".format(local_source_path,bag_name)
        metadata['manifest']=manifest
        #data=read_csv(manifest,sep=" ",usecols=[0,2],names=['md5','filename'],header=None,)
        data=read_csv(manifest,sep="  ",names=['md5','filename'],header=None,)
        metadata['bucket']=s3_bucket
        metadata['verified']=[]
        metadata['error']=[]
        for index, row in data.iterrows():
            bucket_key ="{0}/{1}".format(bag_name,row.filename)
            etag=s3.head_object(Bucket=s3_bucket,Key=bucket_key)['ETag'][1:-1]
            if calculate_multipart_etag("{0}/{1}".format(local_source_path,bucket_key),etag) or etag==row.md5:
                metadata['verified'].append(bucket_key)
            else:
                metadata['error'].append(bucket_key)
        if len(metadata['error'])>0:
            metadata['valid']=False
        else:
            metadata['valid']=True
    else:
        metadata['exists']=False
    cas=db.catalog.bagit_inventory.find_one({'bag':bag_name})
    cas['s3']=metadata
    db.catalog.bagit_inventory.save(cas)
    return {'status':"SUCCESS",'args':[bag_name,local_source_path,s3_bucket,mongo_host],'s3':metadata}
    #return metadata
    
@task()
def validate_norfile_bag(bag_name,local_source_path,mongo_host):
    db=MongoClient(mongo_host)
    inventory_metadata = db.catalog.bagit_inventory.find_one({'bag':bag_name})
    #bag=bagit.Bag('{0}/{1}'.format(local_source_path,bag_name))
    if os.path.isdir('{0}/{1}'.format(local_source_path,bag_name)):
        inventory_metadata['norfile']['exists']=True
        bag=bagit.Bag('{0}/{1}'.format(local_source_path,bag_name))
        if bag.has_oxum():
            inventory_metadata['norfile']['valid']=bag.is_valid(fast=True)
        else:
            try:
                inventory_metadata['norfile']['valid']=bag.validate(processes=4)
            except:
                raise
    else:
        inventory_metadata['norfile']['exists']=False
        inventory_metadata['norfile']['valid']=False
    cas=db.catalog.bagit_inventory.find_one({'bag':bag_name})
    cas['norfile']=inventory_metadata['norfile']
    db.catalog.bagit_inventory.save(cas)
    return {'status':"SUCCESS",'args':[bag_name,local_source_path,mongo_host],'norfile':inventory_metadata['norfile']}

@task()
def clean_nas_files(mongo_host="oulib_mongo"):
    """
    Clean NAS files is a task that checks the bagit inventory catalog for bags that can be deleted from NAS

    """
    db=MongoClient(mongo_host)
    subtasks=[]
    errors=[]
    for itm in db.catalog.bagit_inventory.find({}):
        if itm['s3']['valid'] and itm['norfile']['valid'] and itm['nas']['exists']:
            subtasks.append(itm['bag'])
    for itm in subtasks:
        try:
            remove_nas_files(itm,mongo_host,db)
        except Exception as e:
            errors.append(itm)
    bag_errors=",".join(errors)   
    return "Bags removed: {0}, Bags removal Errors: {1} Bags with Errors:{2} ".format((len(subtasks)-len(errors)),len(errors),bag_errors)

def remove_nas_files(bag_name,mongo_host,db):
    itm = db.catalog.bagit_inventory.find_one({'bag':bag_name})
    if not itm['nas']['location']=="/" and len(itm['nas']['location'])>9:
        status=call(["rm","-rf",itm['nas']['location']])
        #shutil.rmtree(itm['nas']['location'],ignore_errors=True)
        if status == 0:
            itm['nas']['exists']=False
            itm['nas']['place_holder']=True
            open(itm['nas']['location'], 'a').close()
            db.catalog.bagit_inventory.save(itm)
        else:
            itm['nas']['ERROR']="Error removing files: {0}".format(itm['nas']['location'])
            db.catalog.bagit_inventory.save(itm)
            raise Exception("Error removing files: {0}".format(itm['nas']['location']))
    else:
        raise Exception("Security Bag location is suspicious")

@task(bind=True)
def copy_bag(self,bag_name,source_path,dest_path):
    dest="{0}/{1}".format(dest_path,bag_name)
    source = "{0}/{1}".format(source_path,bag_name)
    if os.path.isdir(dest):
        msg = "Bag destination already exists. Host:{0} Destination: {1}".format(mounts_hostname,dest)
        self.update_state(state=states.FAILURE,meta=msg)
        raise Ignore()
        #return "Bag destination already exists. Host:{0} Destination: {1}".format(mounts_hostname,dest)
    if not os.path.isdir(source):
        msg="Bag source directory does not exist. {0}".format(source)
        self.update_state(state=states.FAILURE,meta=msg)
        raise Ignore()
    shutil.copytree(source, dest)
    return "Bag copied from {0} to {1}".format(source,dest)

@task(bind=True)
def upload_bag_s3(self,bag_name,source_path,s3_bucket='ul-bagit'):
    """
    AWS CLI tool must be installed and aws keys setup
    """
    task_id = str(upload_bag_s3.request.id)
    source ="{0}/{1}".format(source_path,bag_name)
    s3_loc = "s3://{0}/{1}".format(s3_bucket,bag_name)
    log=open("{0}.tmp".format(task_id),"w+")
    status=call(['/env/bin/aws','s3','sync',source,s3_loc],stderr=log) 
    if status != 0:
        log.seek(0)
        msg= log.read()
        log.close()
        os.remove("{0}.tmp".format(task_id))
        self.update_state(state=states.FAILURE,meta=msg)
        raise Ignore()
    else:
        msg="Bag uploaded from {0} to {1}".format(source,s3_loc)
        
    return msg
  
def calculate_multipart_etag(source_path,etag, chunk_size=8):

    """
    calculates a multipart upload etag for amazon s3
    Arguments:
        source_path -- The file to calculate the etag 
        etag -- s3 etag to compare
    Keyword Arguments:
        chunk_size -- The chunk size to calculate for. Default 8
    """
    md5s = []
    chunk_size = chunk_size * 1024 * 1024
    with open(source_path,'rb') as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))
    digests = b"".join(m.digest() for m in md5s)
    new_md5 = hashlib.md5(digests)
    new_etag = '%s-%s' % (new_md5.hexdigest(),len(md5s))
    #print source_path,new_etag,etag
    if etag==new_etag:
        return True
    else:
        return False
