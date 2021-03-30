from celery.task import task
from .utils import get_metadata, upsert_metadata
from .utils import is_bag_valid


@task()
def replicate(bag):
    tasks = None
    if is_bag_valid(bag):
        norfile_exists = in_norfile(bag)
        norfile_valid = validate_norfile(bag) if norfile_exists else False
        s3_exists = in_s3(bag)
        s3_valid = validate_s3(bag) if s3_exists else False

        if norfile_valid and s3_valid:
            remove_nas(bag)

        if not norfile_exists:
            copy(bag, source, destination)
        elif norfile_exists and not norfile_valid:
            validate_norfile(bag)

        if not s3_exists(bag):
            upload_s3(bag, source, bucket)
        elif s3_exists and not s3_valid:
            validate_s3(bag)


@task()
def replicate_bag(bag, project=None, department=None, force=None, celery_queue="digilab-nas2-prod-workerq"):
    """Chain bag replication

    """
    # Check to see if bag exists
    nas_bagit, norfile_bagit, s3_bucket, s3_key, s3_folder = _find_bag(bag)
    inventory_metadata = get_metadata(bag)
    # update project and department if available
    if project:
        inventory_metadata['project'] = project
    if department:
        inventory_metadata['department'] = department
    # save inventory metadata
    upsert_metadata(inventory_metadata)
    # setup workflow chain
    subtasks = []
    bag_chain = []



    # norfile validation
    if not inventory_metadata['locations']['norfile']['valid'] or force:
        bag_chain.append(copy_bag.si(bag, nas_bagit, norfile_bagit).set(queue=celery_queue))
        subtasks.append(validate_norfile_bag.si(bag, norfile_bagit).set(queue=celery_queue))
    #  s3 validataion
    if not inventory_metadata['locations']['s3']['valid'] or force:
        bag_chain.append(upload_bag_s3.si(bag, nas_bagit, s3_bucket, s3_key).set(queue=celery_queue))
        subtasks.append(
            validate_s3_files.si(bag, norfile_bagit, s3_bucket, s3_base_key=s3_folder).set(queue=celery_queue))
    # nas validation
    subtasks.append(validate_nas_files.si(bag, nas_bagit).set(queue=celery_queue))

    if len(bag_chain) == 2:
        cp_val_chain = (bag_chain[0] | bag_chain[1] | subtasks[0] | subtasks[1] | subtasks[2] | clean_nas_files.si(
            bag=bag).set(queue=celery_queue))()
    elif len(bag_chain) == 1:
        cp_val_chain = (
                    bag_chain[0] | subtasks[0] | subtasks[1] | clean_nas_files.si(bag=bag).set(queue=celery_queue))()
    else:
        cp_val_chain = (subtasks[0] | clean_nas_files.si(bag=bag).set(queue=celery_queue))()

    return "Replication workflow started for bag {0}. Please see child subtasks for workflow result.".format(bag)
