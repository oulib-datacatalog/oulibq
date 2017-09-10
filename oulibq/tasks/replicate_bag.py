from celery.task import task
import os,requests,json
from bag_migration import get_celery_worker_config,copy_bag,upload_bag_s3
from tasks import clean_nas_files,validate_nas_files,validate_s3_files,validate_norfile_bag
import ConfigParser
import logging
from datetime import datetime

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
    returns NAS Path , Norfile Path, S3 Bucket,S3 Key, S3 folder
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
    return nas_path, nas_config["norfile"]["bagit"],nas_config["s3"]["bucket"],os.path.join(s3_folder,bag),s3_folder

def _get_bags(path,folder):
    #s3_folder="private/private"
    pvlocation=os.path.join(path,folder)
    return [os.path.join(folder,name) for name in os.listdir(pvlocation) if os.path.isdir(os.path.join(pvlocation, name,'data'))]
        
@task()
def replicate_bag(bag, project=None, department=None, force=None, celery_queue="digilab-nas2-prod-workerq"):
    """Chain bag replication

    """
    # Check to see if bag exists
    nas_bagit,norfile_bagit,s3_bucket,s3_key,s3_folder =  _find_bag(bag)
    #setup data catalog
    data = _api_get(bag)
    if data['count']>0:
        inventory_metadata = data['results'][0]
    else:
        #new item to inventory
        inventory_metadata={ 'derivatives':{},'project':'','department':'', 'bag':bag,'locations':{
                            's3':{'exists':False,'valid':False,'bucket':'','validation_date':'','manifest':'','verified':[],'error':[]},
                            'norfile':{'exists':False,'valid':False,'validation_date':'','location':'UL-BAGIT'},
                            'nas':{'exists':False,'place_holder':False,'location':''}}}
    #update project and department if available 
    if project:
        inventory_metadata['project']=project
    if department:
        inventory_metadata['department']=department
    #save inventory metadata
    _api_save(inventory_metadata)
    # setup workflow chain
    subtasks=[]
    bag_chain=[]
    
    # norfile validation
    if not inventory_metadata['locations']['norfile']['valid'] or force:
        bag_chain.append(copy_bag.si(bag,nas_bagit,norfile_bagit).set(queue=celery_queue)) 
        subtasks.append(validate_norfile_bag.si(bag,norfile_bagit).set(queue=celery_queue))
    #  s3 validataion
    if not inventory_metadata['locations']['s3']['valid'] or force:
        bag_chain.append(upload_bag_s3.si(bag,nas_bagit,s3_bucket,s3_key).set(queue=celery_queue))
        subtasks.append(validate_s3_files.si(bag,norfile_bagit,s3_bucket,s3_base_key=s3_folder).set(queue=celery_queue))
    # nas validation
    subtasks.append(validate_nas_files.si(bag,nas_bagit).set(queue=celery_queue))
    
    if len(bag_chain)==2:        
        cp_val_chain = (bag_chain[0]|bag_chain[1]|subtasks[0]|subtasks[1]|subtasks[2]|clean_nas_files.si().set(queue=celery_queue))()
    elif len(bag_chain)==1:
        cp_val_chain = (bag_chain[0]|subtasks[0]|subtasks[1]|clean_nas_files.si().set(queue=celery_queue))()
    else:
        cp_val_chain = (subtasks[0] | clean_nas_files.si().set(queue=celery_queue))()

    return "Replication workflow started for bag {0}. Please see child subtasks for workflow result.".format(bag)
def _filterbags(bags,order,bagspergroup=2):
    if not bags:
        return [],[]
    elif len(bags)>=bagspergroup:
        if order == -1:
            return bags[(bagspergroup*order):], bags[:(bagspergroup*order)]
        else:
            return bags[:bagspergroup], bags[bagspergroup:]
    else:
        return bags,[]

@task()
def managed_replication(number_of_tasks=15):
    """
    Task checks both NAS Locations and creates a celery group of replicate_bag subtasks.
    kwargs:
        number_of_tasks - default 15

    The task will check in each location and sublocation(private,preservation,shareok) and run the first two from each list. If 
    space left to run more tasks. Will add the remaining tasks to the total number. The order is determined by day of year (odd 
    reverse order).   
    """
    tasks=[]
    remaining=[]
    #set order 
    if datetime.now().timetuple().tm_yday % 2==0:
        order=1
    else:
        order=-1 
    #set nas locations
    nas_config= get_celery_worker_config("deprecated-value")
    nas1=nas_config["nas"]["bagit"]
    nas2= nas_config["nas"]["bagit2"]
    for loc in ["preservation","private","shareok",""]:
        #nas1
        temp,tempremain = _filterbags(_get_bags(nas1,loc),order)
        tasks=tasks + temp
        remaining= remaining + tempremain
        #nas2
        temp,tempremain = _filterbags(_get_bags(nas2,loc),order)
        tasks=tasks + temp
        remaining= remaining + tempremain
    # Check if add remaining tasks
    if len(tasks) < number_of_tasks:
        if order==1:
            tasks = tasks + remaining[:number_of_tasks-len(tasks)]
            remaining = remaining[number_of_tasks-len(tasks):]
        else:
            tasks = tasks + remaining[(number_of_tasks-len(tasks))*order:]
            remaining = remaining[:(number_of_tasks-len(tasks))*order]
    #Create subtasks
    subtasks=[]
    for bag in tasks:
        subtasks.append(replicate_bag.si(bag))
    #submit group of tasks
    group(subtasks)()
    return "Replication workflow started: {0}, Bags: {1}   Remaining Bags to replicate {2}".format(len(tasks),tasks,len(remaining))
        
def replicated_bag_mv(bag,bag_dest):
    """
        args:
            bag (bag name or path to bag: bagname or preservation/bagname or shareok/bagname or private/bagename)
            bag_dest (bagname shareok/bagname or private/bagename or preservation/bagname) 
    """
    data = _api_get(bag)
    inventory_metadata=None
    if data['count']>0:
        inventory_metadata = data['results'][0]
    else:
        raise Exception("Bag was not found within data catalog")
    #Check that replication has completed
    if inventory_metadata['locations']['nas']['exists'] or not inventory_metadata['locations']['norfile']['valid'] or not inventory_metadata['locations']['s3']['valid']:
        raise Exception("Initial replication process is not complete. Please finish process then run this task again.")

    # Norfile move operation
    if 'shareok/' in bag_dest or 'preservation/' in bag_dest or 'private/' in bag_dest or len(bag_dest.split('/'))<=2:
        nas_config= get_celery_worker_config("deprecated-value")
        source = os.path.join(nas_config["norfile"]["bagit"],bag)
        dest = os.path.join(nas_config["norfile"]["bagit"],bag_dest) 
        return "{0} - {1}".format(source, dest)
        # mv or rename bag
        #call(['mv',source, dest])
        #raise 



