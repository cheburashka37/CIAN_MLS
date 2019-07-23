#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example high-level Kafka 0.9 balanced Consumer
#

from confluent_kafka import Consumer, KafkaException
import sys
import getopt
import json
import logging
from pprint import pformat


# Consumer configuration
conf = {'bootstrap.servers': '10.156.0.3:6667', 'group.id': 'sschokorov'}

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
			f = open("consumer_data/" + str(msg.topic()) + "_" + str(msg.partition()) + "_" + str(msg.offset()) + "_" + str(msg.key()) + ".txt", "w") 
			f.write(str(msg.value()))
			print("ok")
			f.close()
except Exception as ex:
	print("Consumer:: error in poll():")
	print(ex)

finally:
	# Close down consumer to commit final offsets.
	c.close()
	if f is not None:
		f.close()
