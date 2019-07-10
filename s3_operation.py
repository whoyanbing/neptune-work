import boto3

def list_file(bucket_name='load-data-to-neptune'):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        objs = bucket.meta.client.list_objects(Bucket=bucket_name)
        for file in objs['Contents']:
                print(file['Key'])

def delete_file(bucket_name='load-data-to-neptune', key_name):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        bucket.meta.client.delete_object(Bucket=bucket_name, Key=key_name)

def create_file(bucket_name='load-data-to-neptune', key_name):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        bucket.meta.client.put_object(Bucket=bucket_name, Key=key_name)

