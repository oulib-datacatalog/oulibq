#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='oulibq',
      version='0.2.1',
      packages=find_packages(),
      install_requires=[
          'celery==3.1.22; python_version == "2.7"',
          'pymongo==3.2.1; python_version == "2.7"',
          'celery==4.4.7; python_version >= "3.6"',
          'pymongo==3.11.3; python_version >= "3.6"',
          'requests==2.24.0',
          'boto3==1.16.51',
          'bagit==1.7.0',
      ],
)
