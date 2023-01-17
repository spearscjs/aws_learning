import boto3
from config import s3

session = boto3.Session(profile_name=s3.profile_name)
client = session.client('s3')
s3 = session.resource('s3')

def empty_bucket(bucket_name : str):
    """
    empty specified bucket 

    :param bucket_name: name of AWS bucket 
    """
    s3.Bucket(bucket_name).objects.delete() 
    

def bucket_exists(bucket_name : str):
    """
    check if a bucket exists in the connected session

    :param bucket_name: name of AWS bucket 
    :return: boolean to specify if bucket was found
    :rtype: boolean
    """
    raw = client.list_buckets()
    buckets = raw['Buckets']
    for b in buckets:
        if b['Name'] == bucket_name: return True
    return False


def create_bucket( bucket_name : str ):
    """
    create bucket in the connected session if it does not exist

    :param bucket_name: name of AWS bucket 
    :return: boolean to specify if bucket was found
    :rtype: boolean
    """
    if(not bucket_exists( bucket_name )):
        client.create_bucket(bucket_name)   


def upload_file(file_name : str, bucket_name : str, object_name=None):
    """
    upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    s3.meta.client.upload_file(file_name, bucket_name, object_name)