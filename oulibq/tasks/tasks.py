from celery.task import task
from celery import states
from celery.exceptions import Ignore
from celery.task.sets import TaskSet
#from dockertask import docker_task
from subprocess import call,STDOUT
#import requests
import os, hashlib, bagit
#from pymongo import MongoClient
import boto3,shutil,requests,json
from pandas import read_csv
#Default base directory
#basedir="/data/static/"
from bag_migration import get_celery_worker_config
import ConfigParser
import logging

logging.basicConfig(level=logging.INFO)

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

@task()
def digilab_inventory(bags=None,force=None,project=None,department=None,celery_queue="digilab-nas2-prod-workerq"):
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
    """
    #Celery Worker storage connections
    celery_config = get_celery_worker_config('cc.lib.ou.edu')
    #set variables
    #nas_bagit=[celery_config['nas']['bagit2'],celery_config['nas']['bagit']]
    nas_bagit = celery_config['nas']['bagit2']
    norfile_bagit=celery_config['norfile']['bagit']
    s3_bucket=celery_config['s3']['bucket']
    #get list of bags from norfile
    valid_bags=[]
    if bags:
        valid_bags=bags.split(',')
    else:
        valid_bags=[name for name in os.listdir(norfile_bagit) if os.path.isdir(os.path.join(norfile_bagit,name,'data'))]
    #remove hidden folders
    valid_bags = [x for x in valid_bags if not x.startswith(('_','.'))]
    #variables
    subtasks=[]
    new_cat=0
    update_cat=0
    for bag in valid_bags:
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
            subtasks.append(validate_s3_files.subtask(args=(bag,norfile_bagit,s3_bucket),queue=celery_queue))
        # nas validation
        subtasks.append(validate_nas_files.subtask(args=(bag,nas_bagit),queue=celery_queue))
    if subtasks:
        job = TaskSet(tasks=subtasks)
        result_set = job.apply_async()
    return "Bag Inventory: {0} New, {1} Updates. {2} subtasks submitted".format(new_cat,update_cat,len(subtasks))


@task()
def validate_nas_files(bag_name,local_source_paths):
    """
    Validation of NAS

    Agrs:
        bag_name - string
        local_source_paths - list of sources
    """
    data = _api_get(bag_name)
    #db=MongoClient(mongo_host)
    inventory_metadata = data['results'][0]
    #db.catalog.bagit_inventory.find_one({'bag':bag_name})
    #location=0
    #for local_source_path in local_source_paths:
    local_source_path=os.path.join(local_source_paths,bag_name)
    if os.path.isdir(local_source_path):
        inventory_metadata['locations']['nas']['exists']=True
        inventory_metadata['locations']['nas']['place_holder']=False
        inventory_metadata['locations']['nas']['location']=local_source_path
        inventory_metadata['locations']['nas']['error']=""
        #location+=1
    elif os.path.exists(local_source_path):
        inventory_metadata['locations']['nas']['exists']=False
        inventory_metadata['locations']['nas']['place_holder']=True
        inventory_metadata['locations']['nas']['location']=local_source_path
        inventory_metadata['locations']['nas']['error']=""
        #location+=1
    #if location>1:
    else:
        inventory_metadata['locations']['nas']['error']="Bag Folder or placeholder not found."
    _api_save(inventory_metadata)
    #cas=db.catalog.bagit_inventory.find_one({'bag':bag_name})
    #cas['nas']=inventory_metadata['nas']
    #db.catalog.bagit_inventory.save(cas)
    return {'status':"SUCCESS",'args':[bag_name,local_source_paths],'nas':inventory_metadata['locations']['nas']}

@task()
def validate_s3_files(bag_name,local_source_path,s3_bucket,s3_base_key='source'):
    """
    Validate s3 files
    args:
        bag_name,local_source_path,s3_bucket
    kwargs:
        s3_base_key='source'
    """
    #Find metadata
    data = _api_get(bag_name)
    inventory_metadata = data['results'][0] #db.catalog.bagit_inventory.find_one({'bag':bag_name})
    metadata=inventory_metadata['locations']['s3']
    # S3 bucket
    s3 = boto3.client('s3')
    s3_key = s3.list_objects(Bucket=s3_bucket, Prefix="{0}/{1}".format(s3_base_key,bag_name),MaxKeys=1)
    if 'Contents' in s3_key:
        metadata['exists']=True
        manifest = "{0}/{1}/manifest-md5.txt".format(local_source_path,bag_name)
        metadata['manifest']=manifest
        #Read manifest
        data=read_csv(manifest,sep="  ",names=['md5','filename'],header=None,)
        metadata['bucket']=s3_bucket
        metadata['verified']=[]
        metadata['error']=[]
        metadata['valid']=False
        for index, row in data.iterrows():
            bucket_key ="{0}/{1}/{2}".format(s3_base_key,bag_name,row.filename)
            local_bucket_key = "{0}/{1}".format(bag_name,row.filename)
            etag=s3.head_object(Bucket=s3_bucket,Key=bucket_key)['ETag'][1:-1]
            if calculate_multipart_etag("{0}/{1}".format(local_source_path,local_bucket_key),etag) or etag==row.md5:
                metadata['verified'].append(bucket_key)
            else:
                metadata['error'].append(bucket_key)
        if len(metadata['error'])==0:
            metadata['valid']=True
    else:
        metadata['exists']=False
    inventory_metadata['locations']['s3']=metadata
    #Save metadata
    _api_save(inventory_metadata)
    return {'status':"SUCCESS",'args':[bag_name,local_source_path,s3_bucket],'s3':metadata}
    #return metadata

@task()
def validate_norfile_bag(bag_name,local_source_path):
    """
    Validate Norfile bag
    args:
        bag_name,local_source_path
    """
    #Find metadata
    data = _api_get(bag_name)
    inventory_metadata = data['results'][0]
    #bag=bagit.Bag('{0}/{1}'.format(local_source_path,bag_name))
    if os.path.isdir('{0}/{1}'.format(local_source_path,bag_name)):
        inventory_metadata['locations']['norfile']['exists']=True
        bag=bagit.Bag('{0}/{1}'.format(local_source_path,bag_name))
        if bag.has_oxum():
            inventory_metadata['locations']['norfile']['valid']=bag.is_valid(fast=True)
        else:
            try:
                inventory_metadata['locations']['norfile']['valid']=bag.validate(processes=4)
            except:
                logging.error("Error validating bag: {0}".format(bag_name))
                raise
    else:
        inventory_metadata['locations']['norfile']['exists']=False
        inventory_metadata['locations']['norfile']['valid']=False
    #Save metadata
    _api_save(inventory_metadata)
    return {'status':"SUCCESS",'args':[bag_name,local_source_path],'norfile':inventory_metadata['locations']['norfile']}

@task()
def clean_nas_files():
    """
    Clean NAS files is a task that checks the bagit inventory catalog for bags that can be deleted from NAS

    """
    #return all digital objects
    data = _api_get("test",return_all=True)
    subtasks=[]
    errors=[]
    for itm in data['results']:
        if itm['locations']['s3']['valid'] and itm['locations']['norfile']['valid'] and itm['locations']['nas']['exists']:
            subtasks.append(itm['bag'])
    for itm in subtasks:
        try:
            remove_nas_files(itm)
        except Exception as e:
            errors.append(itm)
    bag_errors=",".join(errors)
    return "Bags removed: {0}, Bags removal Errors: {1} Bags with Errors:{2} ".format((len(subtasks)-len(errors)),len(errors),bag_errors)

def remove_nas_files(bag_name):
    """
    Remove NAS bag
    agrs:
        bag_name
    """
    data = _api_get(bag_name)
    itm = data['results'][0]
    if not itm['locations']['nas']['location']=="/" and len(itm['locations']['nas']['location'])>9:
        status=call(["rm","-rf",itm['locations']['nas']['location']])
        #Check status
        if status == 0:
            itm['locations']['nas']['exists']=False
            itm['locations']['nas']['place_holder']=True
            #create placeholder
            open(itm['locations']['nas']['location'], 'a').close()
            #Save metadata
            _api_save(itm)
        else:
            itm['locations']['nas']['ERROR']="Error removing files: {0}".format(itm['locations']['nas']['location'])
            #Save metadata
            _api_save(itm)
            logging.error("Error removing files: {0}".format(itm['locations']['nas']['location']))
            raise Exception("Error removing files: {0}".format(itm['locations']['nas']['location']))
    else:
        logging.error("Suspicious Bag location: Security Error - {0}".format(itm['locations']['nas']['location']))
        raise Exception("Suspicious Bag location: Security Error - {0}".format(itm['locations']['nas']['location']))

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
