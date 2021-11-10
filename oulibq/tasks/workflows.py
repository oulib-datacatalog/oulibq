import logging

from celery.task import task
from celery import chain, group, signature
from six import ensure_text
from .config import BAG_LOCATIONS
from .utils import get_metadata, find_bag, BagNotFoundError
from .tasks import validate_nas_files, remove_nas_files, copy_bag, validate_norfile_bag
from .tasks import upload_bag_s3, validate_s3_files

logging.basicConfig(level=logging.INFO)


@task(bind=True)
def replicate(self, bag, project=None, department=None, force=False):
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

    queue_name = self.request.delivery_info['routing_key']
    metadata = get_metadata(bag)
    nas_exists = metadata["locations"]["nas"]["exists"]
    nas_tombstone = metadata["locations"]["nas"]["place_holder"]
    norfile_exists = metadata["locations"]["norfile"]["exists"]
    norfile_valid = metadata["locations"]["norfile"]["valid"]
    s3_exists = metadata["locations"]["s3"]["exists"]
    s3_valid = metadata["locations"]["s3"]["valid"]

    if project:
        metadata["project"] = project
    if department:
        metadata["department"] = department

    # Celery signatures of used tasks
    validate_nas = validate_nas_files.si(bag, nas_path).set(queue=queue_name)
    copy_to_norfile = copy_bag.si(bag, nas_path, norfile_path).set(queue=queue_name)
    validate_norfile = validate_norfile_bag.si(bag, norfile_path).set(queue=queue_name)
    upload_to_s3 = upload_bag_s3.si(bag, nas_path, s3_bucket, s3_key).set(queue=queue_name)
    validate_s3 = validate_s3_files.si(bag, nas_path, s3_bucket, s3_folder).set(queue=queue_name)
    remove_from_nas = remove_nas_files.si(bag).set(queue=queue_name)

    if not norfile_exists and not s3_exists:
        workflow = chain(
            validate_nas,
            group(
                chain(copy_to_norfile, validate_norfile),
                chain(upload_to_s3, validate_s3)
            ),
            remove_from_nas  # TODO: confirm that the workflow stops before here if replication fails above - write tests
        )
    elif (norfile_exists and not norfile_valid) and (s3_exists and not s3_valid):
        workflow = chain(
            validate_nas,
            validate_norfile,
            validate_s3,
            remove_from_nas
        )
    elif norfile_exists and not norfile_valid:
        workflow = chain(
            validate_nas,
            validate_norfile,
            remove_from_nas
        )
    elif s3_exists and not s3_valid:
        workflow = chain(
            validate_nas,
            validate_s3,
            remove_from_nas
        )

    # Kick off workflow
    logging.info(ensure_text("Kicking off replication tasks on {0}: {1}").format(bag, workflow))
    workflow.delay()  # https://docs.celeryproject.org/en/stable/userguide/canvas.html#chains
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
