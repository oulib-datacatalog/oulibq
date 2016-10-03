from celery.task import task
from celery import states
from celery.exceptions import Ignore
#from dockertask import docker_task
from subprocess import call,STDOUT
#import requests
import os, hashlib,bagit
from pymongo import MongoClient
import boto3,shutil
from pandas import read_csv
#Default base directory 
#basedir="/data/static/"
mounts_hostname = "dev-mstacy"

#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result

@task()
def digilab_inventory(bags=None,project='original_bags',department='DigiLab',nas_bagit='/mnt/nas/bagit2',norfile_bagit='/mnt/norfile/UL-BAGIT',s3_bucket='ul-bagit',mongo_host='oulib_mongo'):
    """
    
    """
    #catalog
    db=MongoClient(mongo_host)
    #get list of bags from norfile
    if bags:
        bagits=bags.split(',')
    else: 
        bagits=[name for name in os.listdir(norfile_bagit) if os.path.isdir("{0}/{1}".format(norfile_bagit,name))]
    vailid_bags=[]
    for itm in bagits:
        if os.path.isfile("{0}/{1}/bagit.txt".format(norfile_bagit,itm)):
            valid_bags.append(itm)
    subtasks=[]
    new_cat=0
    update_cat=0
    for bag in valid_bags:
        if db.catalog.bagit_inventory.find({'bag':bag}).count()>0:
            inventory_metadata = db.catalog.bagit_inventory.find_one({'bag':bag})
            update_cat+=1
        else:
            #new item to inventory
            inventory_metadata={'project':project,'department':department, 'bag':bag,'s3_bucket':s3_bucket,
                                's3':{'exists':False,'valid':False,'bucket':'','manifest':'','verified':[],'error':[]},
                                'norfile':{'exists':False,'valid':False,'location':'UL-BAGIT'},
                                'nas':{'exists':False,'place_holder':False,'location':''}}
            new_cat+=1
       
        #save inventory metadata
        db.catalog.bagit_inventory.save(inventory_metadata)
        #norfile
        subtasks.append(validate_norfile_bag.subtask(args=(bag,norfile_bagit,mongo_host)))
        #s3
        subtasks.append(validate_s3_files.subtask(args=(bag,norfile_bagit,s3_bucket,mongo_host)))
        #nas
        subtasks.append(validate_nas_files(args=(bag,nas_bagit,mongo_host)
    if subtasks:
        job = TaskSet(tasks=subtasks)
        result_set = job.apply_async()
    return "Bag Inventory: {0} New, {1} Updates. {2} subtasks submitted".format(new_cat,update_cat,(new_cat + update_cat)*3)

@task()
def validate_nas_files(bag_name,local_source_path,mongo_host):
    db=MongoClient(mongo_host)
    inventory_metadata = db.catalog.bagit_inventory.find_one({'bag':bag_name})
    if os.path.isdir('{0}/{1}'.format(local_source_path,bag_name)):
        inventory_metadata['nas']['exists']=True
        inventory_metadata['nas']['location']='{0}/{1}'.format(nas_bagit,bag)
    elif os.path.exists('{0}/{1}'.format(local_source_path,bag_name)):
        inventory_metadata['nas']['exists']=False
        inventory_metadata['nas']['place_holder']=True
        inventory_metadata['nas']['location']=''
    else:
        inventory_metadata['nas']['exists']=False
        inventory_metadata['nas']['place_holder']=False
        inventory_metadata['nas']['location']=''
    db.catalog.bagit_inventory.save(inventory_metadata)
    return "SUCCESS"

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
        data=read_csv(manifest,sep=" ",usecols=[0,2],names=['md5','filename'],header=None,)
        metadata['bucket']=s3_bucket
        for index, row in data.iterrows():
            bucket_key ="{0}/{1}".format(bag_name,row.filename)
            etag=s3.head_object(Bucket=s3_bucket,Key=bucket_key)['ETag'][1:-1]
            if calculate_multipart_etag("{0}/{1}".format(local_source_path,bucket_key),etag) or etag=row.md5:
                metadata['verified'].append(bucket_key)
            else:
                metadata['error'].append(bucket_key)
    else:
        metadata['exists']=False
    inventory_metadata['s3']=metadata
    db.catalog.bagit_inventory.save(inventory_metadata)
    return "SUCCESS"
    #return metadata
    
@task()
def validate_norfile_bag(bag_name,local_source_path,mongo_host):
    db=MongoClient(mongo_host)
    inventory_metadata = db.catalog.bagit_inventory.find_one({'bag':bag_name})
    bag=bagit.Bag('{0}/{1}'.format(local_source_path,bag_name))
    if os.path.isdir('{0}/{1}'.format(local_source_path,bag_name)):
        inventory_metadata['norfile']['exists']=True
    else:
        inventory_metadata['norfile']['exists']=False
    inventory_metadata['norfile']['valid']=bag.is_valid() 
    db.catalog.bagit_inventory.save(inventory_metadata)
    return "SUCCESS"

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
    task_id = str(teco_spruce_simulation.request.id)
    source ="{0}/{1}".format(source_path,bag_name)
    s3_loc = "s3://{0}/{1}".format(s3_bucket,bag_name)
    log=open("{0}.tmp".format(task_id),"w+")
    status=call(['aws','s3','sync',source,s3_loc],stderr=log) 
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
  
def calculate_multipart_etag(source_path, chunk_size=8, expected=None):

    """
    calculates a multipart upload etag for amazon s3
    Arguments:
        source_path -- The file to calculate the etag for
    Keyword Arguments:
        chunk_size -- The chunk size to calculate for. Default 8
        expected -- If passed a string, the string will be compared and return True or False if match
    """

    md5s = []

    with open(source_path,'rb') as fp:
        while True:

            data = fp.read(chunk_size)

            if not data:
                break
            md5s.append(hashlib.md5(data))

    digests = b"".join(m.digest() for m in md5s)

    new_md5 = hashlib.md5(digests)
    new_etag = '"%s-%s"' % (new_md5.hexdigest(),len(md5s))
    if expected:
        if not expected==new_etag:
            return False
        else:
            return True

    return new_etag    
