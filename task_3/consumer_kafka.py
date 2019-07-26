import sys
import getopt
import json
from pprint import pformat

from confluent_kafka import Consumer, KafkaException

argv = sys.argv
if len(argv) != 2:
    print("enter number of consumers")
    sys.exit(1)

x = argv[1]

# Consumer configuration
conf = {'bootstrap.servers': ['10.156.0.3:6667', '10.156.0.4:6667', '10.156.0.5:6667'], 'group.id': 'sschokorov'}

# Create Consumer instance
# Hint: try debug='fetch' to generate some log messages
c = Consumer(conf)

# Subscribe to topics
c.subscribe(topics = ["mles.announcements"])

# Read messages from Kafka, print to stdout
try:
    while True:
        msg = c.poll()
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            fs = [open("consumer_data/consumer_" + str(i) + ".txt", "w") for i in range(x)]
            for f in fs:
                f.write("topic = " + str(msg.topic()) + "; partition = " + str(msg.partition()) + "; offset = " + str(msg.offset()) + "; key = " + str(msg.key()) + "value = " + str(msg.value()))
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
