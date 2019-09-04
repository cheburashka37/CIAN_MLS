import sys
import getopt
import json
from pprint import pformat

from confluent_kafka import Consumer, KafkaException
from kafka import KafkaClient

try:
    client = KafkaClient(['10.156.0.3:6667', '10.156.0.4:6667', '10.156.0.5:6667'])
    topic_partition_ids = client.get_partition_ids_for_topic('mles.announcements')
    x = len(topic_partition_ids)
    client.close()
except Exception as ex:
    print("client:: error:")
    print(ex)

finally:
    client.close()

# Consumer configuration
conf = {'bootstrap.servers': ['10.156.0.3:6667', '10.156.0.4:6667', '10.156.0.5:6667'], 'group.id': 'sschokorov'}

# Create Consumer instance
xs = [2*i for i in range(x)]
cs = [Consumer(conf) for i in range(x)]

fs = [open("consumer_data/consumer_" + str(i) + ".txt", "w") for i in range(x)]

# Subscribe to topics
for i in range(x):
    cs[i].subscribe(topics = ["mles.announcements"])

# Read messages from Kafka, print to stdout
try:
    while True:
        for i in range(x):
            msg = cs[i].poll()
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                fs[i].write("topic = " + str(msg.topic()) + "; partition = " + str(msg.partition()) + "; offset = " + str(msg.offset()) + "; key = " + str(msg.key()) + "value = " + str(msg.value()))
                print("ok")
                
except Exception as ex:
    print("Consumer:: error:")
    print(ex)

finally:
    # Close down consumer to commit final offsets.
    c.close()
    for f in fs:
        if f is not None:
            f.close()
