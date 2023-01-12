import boto3
from config import s3_connection

session = boto3.Session(profile_name=s3_connection.profile_name)
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
    matches = [b for b in buckets if b['Name'] == bucket_name]
    return len(matches) > 0