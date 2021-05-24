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
from botocore.exceptions import ClientError
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
    #save metadata
    #make sure data is fresh and no overright of metadata
    data = _api_get(bag_name)
    save_meta =data['results'][0]
    save_meta['locations']['nas'] = inventory_metadata['locations']['nas']
    _api_save(save_meta)
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
    s3_key = s3.list_objects(Bucket=s3_bucket, Prefix="{0}/{1}".format(s3_base_key,bag_name).decode('utf-8'),MaxKeys=1)
    if 'Contents' in s3_key:
        metadata['exists']=True
        manifest = "{0}/{1}/manifest-md5.txt".format(local_source_path,bag_name).decode('utf-8')
        metadata['manifest']=manifest
        #Read manifest
        data=read_csv(manifest,sep="  ",names=['md5','filename'],header=None,)
        metadata['bucket']=s3_bucket
        metadata['verified']=[]
        metadata['error']=[]
        metadata['valid']=False
        for index, row in data.iterrows():
            bucket_key = "{0}/{1}/{2}".format(s3_base_key,bag_name,row.filename).decode('utf-8')
            local_bucket_key = "{0}/{1}".format(bag_name,row.filename).decode('utf-8')
            try:
                etag=s3.head_object(Bucket=s3_bucket,Key=bucket_key)['ETag'][1:-1]
            except ClientError:
                errormsg = u"Failed to get S3 object header for key: {0}".format(bucket_key)
                logging.error(errormsg)
                raise Exception(errormsg)
            if calculate_multipart_etag(u"{0}/{1}".format(local_source_path,local_bucket_key),etag) or etag==row.md5:
                metadata['verified'].append(bucket_key)
            else:
                metadata['error'].append(bucket_key)
        if len(metadata['error'])==0:
            metadata['valid']=True
    else:
        metadata['exists']=False
    inventory_metadata['locations']['s3']=metadata
    #Save metadata
    #make sure data is fresh and no overright of metadata
    data = _api_get(bag_name)
    save_meta =data['results'][0]
    save_meta['locations']['s3'] = metadata
    _api_save(save_meta)
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
    #make sure data is fresh and no overright of metadata
    data = _api_get(bag_name)
    save_meta =data['results'][0]
    save_meta['locations']['norfile'] = inventory_metadata['locations']['norfile']
    _api_save(save_meta)
    return {'status':"SUCCESS",'args':[bag_name,local_source_path],'norfile':inventory_metadata['locations']['norfile']}

@task()
def clean_nas_files(bag=None):
    """
    Clean NAS files is a task that checks the bagit inventory catalog for bags that can be deleted from NAS

    """
    #Check if only one bag 
    if bag:
        data = _api_get(bag)
    else:
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
            errors.append(str(e))
    bag_errors=",  ".join(errors)
    return "Bags removed: {0}, Bags removal Errors: {1} Bags with Errors:{2} ".format((len(subtasks)-len(errors)),len(errors),bag_errors)

def remove_nas_files(bag_name):
    """
    Remove NAS bag
    agrs:
        bag_name
    """
    data = _api_get(bag_name)
    itm = data['results'][0]
    if not itm['locations']['nas']['location']=="/" and len(itm['locations']['nas']['location'])>15:
        if not itm['locations']['nas']['exists']:
            raise Exception("Bag: {0} has already been removed".format(bag_name))
             
        try:
            shutil.rmtree(itm['locations']['nas']['location'])
            itm['locations']['nas']['exists']=False
            itm['locations']['nas']['place_holder']=True
            #create placeholder
            open(itm['locations']['nas']['location'], 'a').close()
            #Save metadata
            _api_save(itm)
        except Exception as e:
            #pull metadata to prevent lose of metadata
            data = _api_get(bag_name)
            itm = data['results'][0]
            msg = "Error removing files: {0}. {1}".format(itm['locations']['nas']['location'],str(e))
            itm['locations']['nas']['ERROR']=msg
            #Save metadata
            _api_save(itm)
            logging.error(msg)
            raise Exception(msg)
        #status=call(["rm","-rf",itm['locations']['nas']['location']])
    else:
        logging.error("Suspicious Bag location: Security Error - {0}".format(itm['locations']['nas']['location']))
        raise Exception("Suspicious Bag location: Security Error - {0}".format(itm['locations']['nas']['location']))


def factor_of_1MB(file_size, num_parts):
    x = file_size / int(num_parts)
    y = x % 1048576
    return int(x + 1048576 - y)


def calculate_multipart_etag(source_path,etag,chunk_size=None):

    """
    calculates a multipart upload etag for amazon s3
    Arguments:
        source_path -- The file to calculate the etag
        etag -- s3 etag to compare
    Keyword Arguments:
        chunk_size -- The chunk size to calculate for.
    """

    md5s = []
    if chunk_size:
        chunk_size = chunk_size * 1024 * 1024
    else:
        num_parts = etag.split("-")[-1]
        file_size = os.path.getsize(source_path)
        chunk_size = factor_of_1MB(file_size, num_parts)
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
