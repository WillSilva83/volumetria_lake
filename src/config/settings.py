import os 

class Config: 
    OUTPUTLOCATION = os.getenv('OUTPUTLOCATION', 's3://teste/')
    BUCKET_S3 = os.getenv('BUCKET_S3', "")

config = Config()