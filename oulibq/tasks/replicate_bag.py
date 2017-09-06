from celery.task import task
import os,requests,json
from bag_migration import get_celery_worker_config,copy_bag,upload_bag_s3
from tasks import clean_nas_files,validate_nas_files,validate_s3_files,validate_norfile_bag
import ConfigParser
import logging

from celery import signature,chain,group

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

def _find_bag(bag):
    """ 
    Function returns path to nas and norfile on current worker. Determines if bag is on Nas1 or Nas2.
    returns NAS Path , Norfile Path, S3 Bucket
    """
    nas_config= get_celery_worker_config("deprecated-value")
    if os.path.isdir(os.path.join(nas_config["nas"]["bagit"],bag)):
        nas_path = nas_config["nas"]["bagit"]
    elif os.path.isdir(os.path.join(nas_config["nas"]["bagit2"],bag)):
        nas_path = nas_config["nas"]["bagit2"]
    else:
        raise Exception("Checked both NAS location and unable to find bag:{0}. This task runs rsync. Do not want to run if NAS already deleted and place holder exists.".format(bag))
    if "private" in bag or "preservation" in bag or "shareok" in bag:
        s3_folder="private"
    else:
        s3_folder="source"
    return nas_path, nas_config["norfile"]["bagit"],nas_config["s3"]["bucket"],os.path.join(s3_folder,bag)
        
@task()
def replicate_bag(bag, project=None, department=None, force=None, celery_queue="digilab-nas2-prod-workerq"):
    """Chain bag replication

    """
    # Check to see if bag exists
    nas_bagit,norfile_bagit,s3_bucket,s3_key =  _find_bag(bag)
    data = _api_get(bag)
    if data['count']>0:
        inventory_metadata = data['results'][0]
    else:
        #new item to inventory
        inventory_metadata={ 'derivatives':{},'project':'','department':'', 'bag':bag,'locations':{
                            's3':{'exists':False,'valid':False,'bucket':'','validation_date':'','manifest':'','verified':[],'error':[]},
                            'norfile':{'exists':False,'valid':False,'validation_date':'','location':'UL-BAGIT'},
                            'nas':{'exists':False,'place_holder':False,'location':''}}}
    if project:
        inventory_metadata['project']=project
    if department:
        inventory_metadata['department']=department
    #save inventory metadata
    _api_save(inventory_metadata)
    subtasks=[]
    bag_chain = []
    
    # norfile validation
    if not inventory_metadata['locations']['norfile']['valid'] or force:
        bag_chain.append(copy_bag.si(bag,nas_bagit,norfile_bagit)) 
        subtasks.append(validate_norfile_bag.si(bag,norfile_bagit))
    #  s3 validataion
    if not inventory_metadata['locations']['s3']['valid'] or force:
        bag_chain.append(upload_bag_s3.si(bag,nas_bagit,s3_bucket,s3_key))
        subtasks.append(validate_s3_files.si(bag,norfile_bagit,s3_bucket))
    # nas validation
    subtasks.append(validate_nas_files.si(bag,nas_bagit))
    
    if len(bag_chain)==2:        
        cp_val_chain = (bag_chain[0]|bag_chain[1]|subtasks[0]|subtasks[1]|subtasks[2]|clean_nas_files.si())(queue=celery_queue)
    elif len(bag_chain)==1:
        cp_val_chain = (bag_chain[0]|subtasks[0]|subtasks[1]|clean_nas_files.si())(queue=celery_queue)
    else:
        cp_val_chain = (subtasks[0] | clean_nas_files.si())(queue=celery_queue)

    return "Replication workflow started for bag {0}. Please see child subtasks for workflow result.".format(bag)


