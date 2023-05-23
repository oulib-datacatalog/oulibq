#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='oulibq',
      version='0.1.20',
      packages= find_packages(),
      install_requires=[
          'celery==3.1.22',
          'pymongo==3.2.1',
          'requests==2.31.0',
          'boto3==1.17.112',
          'pandas',
          'bagit==1.8.1',
          'awscli==1.19.112'
      ],
)
