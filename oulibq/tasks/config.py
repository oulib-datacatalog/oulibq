import os

DEFAULT_DAYS_TO_WAIT = 2  # wait this number of days before processing a bag using modified timestamp

paths = [os.getenv("LOCAL_BAGIT_SRC_PATH"), os.getenv("REMOTE_BAGIT_SRC_PATH")]

bag_locations = {
    "s3": {"bucket": "ul-bagit"},
    "nas": {
        "bagit": os.getenv('REMOTE_BAGIT_SRC_PATH', None),
        "bagit2": os.getenv('LOCAL_BAGIT_SRC_PATH', None)
    },
    "norfile": {"bagit": os.getenv('REMOTE_BAGIT_DEST_PATH', None)}
}

private_locations = ['shareok', 'preservation', 'private', 'external-preservation']

inventory_metadata = {
    'derivatives': {},
    'project': '',
    'department': '',
    'bag': '',
    'locations': {
        's3': {
            'exists': False,
            'valid': False,
            'bucket': '',
            'validation_date': '',
            'manifest': '',
            'verified': [],
            'error': []
        },
        'norfile': {
            'exists': False,
            'valid': False,
            'validation_date': '',
            'location': 'UL-BAGIT'
        },
        'nas': {
            'exists': False,
            'place_holder': False,
            'location': ''
        }
    }
}
