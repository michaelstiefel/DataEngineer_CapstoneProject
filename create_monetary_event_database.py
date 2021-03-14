import pandas as pd
import configparser
import boto3

config = configparser.ConfigParser()
config.read("capstone_project.cfg")

AWS_ACCESS_KEY_ID =config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']
S3_BUCKET_EVENTS = config['AWS']['S3_BUCKET_EVENTS']



mpd = pd.read_csv("ecb_decision_dates.csv")


url = "https://www.ecb.europa.eu/press/key/shared/data/all_ECB_speeches.csv?2d214f774ee2c1533ac47f2a3bce3222"
speeches = pd.read_csv(url, sep='|', parse_dates=['date'])
#print(speeches.info())
speeches['type'] = 'speech'
speeches = speeches[['date', 'type', 'speakers']]

df = pd.concat([mpd, speeches])
#print(df.info())
#print(df.head(100))

df.to_csv("ecb_event_data.csv", index = False)



s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

s3.upload_file(Bucket = S3_BUCKET_EVENTS,
               Key = "ecb_event_data.csv",
               Filename = "ecb_event_data.csv")
