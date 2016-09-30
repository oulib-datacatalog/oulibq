from celery.task import task
#from dockertask import docker_task
from subprocess import call,STDOUT
#import requests
import os, hashlib,bagit
from pymongo import MongoClient
import boto3
from pandas import read_csv
#Default base directory 
#basedir="/data/static/"


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
def digilab_inventory(nas_bagit='/mnt/nas/bagit2',norfile_bagit='/mnt/norfile/UL-BAGIT',s3_bucket='ul-bagit',mongo_host='oulib_mongo'):
    """
    
    """
    #catalog
    #db=MongoClient(mongo_host)
    #s3 client
    s3 = boto3.client('s3')#,aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    bagits=[name for name in os.listdir(norfile_bagit) if os.path.isdir(name)]
    vailid_bags=[]
    for itm in bagits:
        if os.path.isfile("{0}/{1}/bagit.txt".format(norfile_bagit,itm)):
             valid_bags.append(itm)
    for bag in valid_bags:
        manifest = "{0}/{1}/manifest-md5.txt".format(norfile_bagit,bag)
        data=read_csv(manifest,sep=" ",usecols=[0,2],names=['md5','filename'],header=None,)
        varified_files=[]
        catalog_template={'project':'','department':'Digilab', 'bag':bag,'manifest':manifest,'s3_bucket':s3_bucket,'s3':{'state':'present','verified':[],'error':[]},
                        'norfile':{'location':'UL-BAGIT','validate':''},'nas':{'state':'exists','location':''}}
        for index, row in data.iterrows():
            bucket_key ="{0}/{1}".format(bag,row.filename)
            try:
                etag=s3.head_object(Bucket=s3_bucket,Key=bucket_key)['ETag'][1:-1]
                if calculate_multipart_etag("{0}/{1}/{2}".format(norfile_bagit,bag,row.filename),etag) or etag==row.md5:
                    catalog_template['s3']['verified'].append(bucket_key)
                else:
                    catalog_template['s3']['error'].append(bucket_key)
            except:
                
        norbag=bagit.Bag('{0}/{1}'.format(norfile_bagit,bag))
        if norbag.is_valid():
            catalog_template['norfile']['validate']='Valid'
        else:
            catalog_template['norfile']['validate']='Not Valid'
        print catalog_template
        
#return bagits
    
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
