import logging

from celery.task import task
from celery import chain
from six import ensure_text
from .config import BAG_LOCATIONS
from .utils import get_metadata, find_bag, BagNotFoundError,
from .tasks import validate_nas_files, remove_nas_files, copy_bag, validate_norfile_bag
from .tasks import upload_bag_s3, validate_s3_files

logging.basicConfig(level=logging.INFO)


@task()
def replicate(bag, project=None, department=None, force=False):
    """
    This task validates and replicates a single bag to Norfile and AWS S3

    args:
        bag - string name of bag to replicate
        project - optional string to tag project
        department - optional string to tag department
        force - optional boolean to force replication steps
    """
    try:
        nas_path, norfile_path, s3_bucket, s3_key, s3_folder = find_bag(bag)
    except BagNotFoundError:
        logging.exception(ensure_text("Could not find bag: {0}").format(bag))
        return {"error": "Could not find specified bag!"}

    metadata = get_metadata(bag)
    norfile_exists = metadata["locations"]["norfile"]["exists"]
    norfile_valid = metadata["locations"]["norfile"]["valid"]
    s3_exists = metadata["locations"]["s3"]["exists"]
    s3_valid = metadata["locations"]["s3"]["valid"]

    if project:
        metadata["project"] = project
    if department:
        metadata["department"] = department

    # Celery signatures of used tasks
    validate_nas = validate_nas_files.si(bag, nas_path)
    copy_to_norfile = copy_bag.si(bag, nas_path, norfile_path)
    validate_norfile = validate_norfile_bag.si(bag, norfile_path)
    upload_to_s3 = upload_bag_s3.si(bag, nas_path, s3_bucket, s3_key)
    validate_s3 = validate_s3_files.si(bag, nas_path, s3_bucket, s3_folder)
    remove_from_nas = remove_nas_files.si(bag)

    # Build up list of tasks to perform
    actions = [validate_nas]

    if not norfile_exists or force:
        actions.extend((copy_to_norfile, validate_norfile))
    elif norfile_exists and not norfile_valid:
        actions.append(validate_norfile)

    if not s3_exists or force:
        actions.extend((upload_to_s3, validate_s3))
    elif s3_exists and not s3_valid:
        actions.append(validate_s3)

    if norfile_valid and s3_valid:
        actions.append(remove_from_nas)

    # Kick off workflow
    logging.info(ensure_text("Kicking off replication tasks on {0}: {1}").format(bag, actions))
    chain(actions).delay()  # https://docs.celeryproject.org/en/stable/userguide/canvas.html#chains
    return {"ok": "Kicked off replication!"}


@task()
def managed_replication():
    """
    This task searches for bags ready for replication and kicks off the replication workflow
    """
    # TODO add code to find bags and confirm kick off of replication tasks
    bags = []  # find subset of bags ready for replication
    logging.info(ensure_text("Kicking off managed replications of: {0}").format(bags))
    for bag in bags:
        replicate.si(bag).delay()
    return {"ok": "Kicked off managed replications!"}
