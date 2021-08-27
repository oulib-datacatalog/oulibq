#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='oulibq',
      version='0.1.14',
      packages= find_packages(),
      install_requires=[
          'celery==3.1.22',
          'pymongo==3.2.1',
          'requests==2.24.0',
          'boto3==1.4.0',
          'pandas',
          'bagit==1.5.4',
          'awscli==1.11.0'
      ],
)
