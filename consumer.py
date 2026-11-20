
import boto3
import time

stream_name = "TripEventsStream"

kinesis = boto3.client(
    "kinesis",
    region_name="ap-south-1"
)

response = kinesis.list_shards(
    StreamName=stream_name
)

shard_id = response["Shards"][0]["ShardId"]

print("Shard ID:", shard_id)

iterator_response = kinesis.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType="TRIM_HORIZON"
)

shard_iterator = iterator_response["ShardIterator"]

empty_count = 0

while shard_iterator:

    response = kinesis.get_records(
        ShardIterator=shard_iterator,
        Limit=100
    )

    records = response["Records"]

    if records:
        empty_count = 0

        for record in records:
            print("Partition Key:", record["PartitionKey"])
            print("Sequence Number:", record["SequenceNumber"])
            print("----------------------")

    else:
        empty_count += 1

    shard_iterator = response["NextShardIterator"]

    if empty_count >= 10:
        break

    time.sleep(1)

print("Consumer finished reading records!")
