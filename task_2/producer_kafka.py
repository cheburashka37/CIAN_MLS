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
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#

from confluent_kafka import Producer
from random import randint
import sys
import json
import datetime

topic = 'sschokorov'

# Producer configuration
conf = {'bootstrap.servers': '10.156.0.3:6667',
		'bootstrap.servers': '10.156.0.4:6667',
		'bootstrap.servers': '10.156.0.5:6667'}

# Create Producer instance
p = Producer(**conf)

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).

# Read lines from stdin, produce each line to Kafka
for e in range(10):
	try:
		# Produce line (without newline)
		a = randint(1, 100)
		line = str(a)
		b = datetime.datetime.now()
		x = '{"timestamp":"' + str(b)[:-3] + '", "message":"msg"}'
		print(x)			
		y = json.loads(x)
		p.produce(topic, key = line, value = str(y))


	except BufferError:
		sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try 			again\n' % len(p))

# Serve delivery callback queue.
# NOTE: Since produce() is an asynchronous API this poll() call
#       will most likely not serve the delivery callback for the
#       last produce()d message.
	p.poll(0)

# Wait until all messages have been delivered
p.flush()
