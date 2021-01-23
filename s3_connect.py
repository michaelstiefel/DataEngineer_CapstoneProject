import boto3
import configparser

config = configparser.ConfigParser()
config.read("capstone_project.cfg")

AWS_ACCESS_KEY_ID =config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']
S3_Bucket = config['AWS']['S3_BUCKET']

# Generate the boto3 client for interacting with S3
s3 = boto3.client('s3', region_name ='us-east-2',
                        # Set up AWS credentials
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
# List the buckets
buckets = s3.list_buckets()

# Print the buckets
print(buckets)
