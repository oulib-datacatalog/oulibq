#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='oulibq',
      version='0.2.4',
      packages=find_packages(),
      install_requires=[
          'celery==3.1.22; python_version == "2.7"',
          #'celery==4.4.7; python_version >= "3.6"',
          'celery==5.1.2; python_version >= "3.6" and python_version < "3.7"',
          'celery==5.2.1; python_version >= "3.7"',
          'boto3==1.17.112; python_version == "2.7"',
          'boto3==1.20.12; python_version >= "3.6" and python_version < "3.7"',
          'boto3==1.26.78; python_version >= "3.7"',
          'pymongo==3.12.1',
          'requests==2.26.0',
          'bagit==1.8.1',
          'awscli==1.19.112; python_version == "2.7"',
          'awscli==1.27.78; python_version >= "3.7"',
          'six==1.16.0',
      ],
)
