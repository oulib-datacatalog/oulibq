import os
import pytest
import boto3
from moto import mock_s3


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture(scope="function")
def s3_resource(aws_credentials):
    with mock_s3():
        s3_resource = boto3.resource('s3', region_name='us-east-1')
        yield s3_resource


@pytest.fixture(scope="function")
def s3_resource_bucket(s3_resource, default_environ):
    bucket = os.environ['DEFAULT_BUCKET']
    s3_resource.create_bucket(Bucket=bucket)
    yield s3_resource


@pytest.fixture(scope="function")
def default_environ():
    os.environ['DEFAULT_BUCKET'] = 'test-bucket' 

           
@pytest.fixture(scope="function")
def s3_test_bucket(s3_client, default_environ):
    bucket = os.environ['DEFAULT_BUCKET']
    s3_client.create_bucket(Bucket=bucket)
    yield s3_client