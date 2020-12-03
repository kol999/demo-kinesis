import boto3 
from faker import Faker
import random
import time
import json 

DELIVERY_STREAM='firehose_article'


client = None

try: 
    client = boto3.client('firehose',region_name='eu-west-1')
except Exception as e:
    print(e)

fake = Faker() 
captains = [ 
   'Jean-Luc Picard', 
   'James T. Kirk', 
   'Christopher Park', 
   'Captain Marvel' 
    ]


record = {} 
while True:
    record['user'] = fake.name() 
    record['captain'] = random.choice(captains) 
    record['timestamp'] = time.time()  
    response = client.put_record(
        # DeliveryStreamName=DELIVERY_STREAM, 
        DeliveryStreamName='firehose-article', 
        Record = {
         'Data' : json.dumps(record) + '\n'
        }
    )
    print('Record: \n' + str(record))

