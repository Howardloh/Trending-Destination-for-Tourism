from kafka import KafkaConsumer
from json import loads, dumps
from time import sleep
import happybase, time, hashlib, struct, threading
from datetime import datetime


def get_UTC_timestamp() -> int:
    dt = datetime.datetime.now()
    return int(dt.strftime("%Y%m%d%H%M%S"))


def store_hbase(i: int, hbase_table: happybase.Table, message : str) -> None:
    print(message.value)
    row_hash = hashlib.sha1(str(i).encode()).hexdigest()
    hbase_table.put(row_hash, {'cf1:edited': str(message.value["edited"]), 
                               'cf1:spoiler': str(message.value["spoiler"]),
                               'cf2:url': message.value["url"],
                               'cf2:name': message.value["name"],
                               'cf2:sr_title': message.value["title"],
                               'cf3:created_utc': str(message.value["created_utc"]),
                               'cf3:num_comments': str(message.value["num_comments"]),
                               'cf3:score': str(message.value["score"])})
    print(f"stored {row_hash} into hbase metastore : {i}")


def get_hbase():
    connection = happybase.Connection(port=9090)
    try:
        connection.create_table('redditTravel', {'cf1':dict(), 
                                                'cf2':dict(),
                                                'cf3':dict()})
    except:
        print("table already created")

    return connection.table('redditTravel')


def get_stream():  
    consumer = KafkaConsumer(
        bootstrap_servers = 'localhost:9092',
        value_deserializer = lambda x : loads(x.decode('utf-8'))
    )  
    print('Kafka Consumer has been initiated...')

    consumer.subscribe(['redditStreams']) #redditStream -> name of created topic
    
    hbase_table = get_hbase()
    
    for i, message in enumerate(consumer):
        store_hbase(i, hbase_table, message)
        
if __name__ == '__main__':
    get_stream()