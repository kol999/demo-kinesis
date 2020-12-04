import boto3
import json
from datetime import datetime
import time
import sys

SHARD_NUMBER=0 
if len(sys.argv) > 1:
    print(len(sys.argv)) 
    SHARD_NUMBER = (int(sys.argv[1]) - 1) 
else:
    print("No arg passed") 
    print("You can run this per shard by passing the shard number to read e.g. 1 or 2") 

# print("SHARD_NUMBER:" + str(SHARD_NUMBER)) 

STREAM_NAME = 'sensor-stream2'
POSITION='TRIM_HORIZON' 


kinesis_client = boto3.client('kinesis', region_name='eu-west-1')
response = kinesis_client.describe_stream(StreamName=STREAM_NAME) 

my_shard_id = response['StreamDescription']['Shards'][SHARD_NUMBER]['ShardId']
shard_iterator = kinesis_client.get_shard_iterator(StreamName=STREAM_NAME,
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType=POSITION)

my_shard_iterator = shard_iterator['ShardIterator']

# record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator, Limit=2)
record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator) 

for rec in record_response['Records']:
    print(rec['PartitionKey'])

print("No. of records retrieved:" + str(len(record_response['Records']))) 

while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator) 

    for rec in record_response['Records']:
        print(rec['PartitionKey'])
    print("No. of records retrieved:" + str(len(record_response['Records']))) 

    # print(record_response) 
    # wait for 5 seconds
    print("Sleeping for 5 secs...") 
    time.sleep(1)
    print("Sleeping for 4 secs...") 
    time.sleep(1)
    print("Sleeping for 3 secs...") 
    time.sleep(1)
    print("Sleeping for 2 secs...") 
    time.sleep(1)
    print("Sleeping for 1 secs...") 
    time.sleep(1)
