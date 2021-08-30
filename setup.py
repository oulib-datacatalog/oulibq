#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='oulibq',
      version='0.1.16',
      packages= find_packages(),
      install_requires=[
          'celery',
          'pymongo',
          'requests==2.26.0',
          'boto3==1.17.112',
          'pandas',
          'bagit==1.8.1',
          'awscli==1.19.112'
      ],
)
