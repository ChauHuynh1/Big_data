# How to Build a Distributed Big Data Pipeline Using Kafka, Cassandra, and Jupyter Lab with Docker

You can use the resources in this github to deploy an end-to-end data pipeline on your local computer using Docker containerized Kafka (data streaming), Cassandra (NoSQL database) and Jupyter Lab (data analysis visualization).

This bases on the repo https://github.com/salcaino/sfucmpt733/tree/main/foobar-kafka
Substantial changes and bug fixes have been made. Tested on Windows 10 and BigSur version 11.5. 

# Preparation for this task

You need to apply for some APIs to use with this. The APIs might take days for application to be granted access. Sample API keys are given, but it can be blocked if too many users are running this.

Twitter Developer API: https://developer.twitter.com/en/apply-for-access

OpenWeatherMap API: https://openweathermap.org/api 

After obtaining the API keys, please update the files  "twitter-producer/twitter_service.cfg" and "owm-producer/openweathermap_service.cfg" accordingly.

## IMPORTANT!!: You must install mimesis first!!
```
pip install mimesis
```

#Create docker networks
```bash
$ docker network create kafka-network                         # create a new docker network for kafka cluster (zookeeper, broker, kafka-manager services, and kafka connect sink services)
$ docker network create cassandra-network                     # create a new docker network for cassandra. (kafka connect will exist on this network as well in addition to kafka-network)
```
## Setting up for Cassandra

Add these to a new schema file "cassandra/schema-another.cql"

```
USE kafkapipeline;
CREATE TABLE IF NOT EXISTS kafkapipeline.anotherdata (
  dna_sequence TEXT,
  rna_sequence TEXT,
  software_license TEXT,
  version TEXT,
  programming_language TEXT,
  os TEXT,
  issn TEXT,
  isbn TEXT,
  ean TEXT,
  pin TEXT,
  PRIMARY KEY (dna_sequence)
);
```

Now, add that file to the Dockerfile COPY line accordingly and rebuild the docker container Cassandra with the tag --build
```bash
$ docker-compose -f cassandra/docker-compose.yml up -d --build
```

Then go inside the Cassandra container and run 
```
cqlsh -f schema-another.cql
```

## Setting up for Kafka

In the folder "kafka", add this to the "connect/create-cassandra-sink.sh"

```
echo "Done."

echo "Starting Another Sink"
curl -s \
     -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d '{
  "name": "anothersink",
  "config": {
    "connector.class": "com.datastax.oss.kafka.sink.CassandraSinkConnector",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",  
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable":"false",
    "tasks.max": "10",
    "topics": "another",
    "contactPoints": "cassandradb",
    "loadBalancing.localDc": "datacenter1",
    "topic.another.kafkapipeline.anotherdata.mapping": "dna_sequence=value.dna_sequence, rna_sequence=value.rna_sequence, software_license=value.software_license, version=value.version, programming_language=value.programming_language, os=value.os, issn=value.issn, isbn=value.isbn, ean=value.ean, pin=value.pin",
    "topic.another.kafkapipeline.anotherdata.consistencyLevel": "LOCAL_QUORUM"
  }
}'
echo "Done."
```

Now, rebuild the docker container kafka with the tag --build

```
docker-compose -f kafka/docker-compose.yml up -d --build
```

> **Note:** 
Kafka-Manager front end is available at http://localhost:9000

You can use it to create cluster to view the topics streaming in Kafka.


IMPORTANT: You have to manually go to CLI of the "kafka-connect" container and run the below comment to start the Cassandra sinks.
```
./start-and-wait.sh
```

## Starting Producers

### Setting up for Faker

Duplicate the "own-producer" folder to get us some guiding code, rename it to "faker-producer". Inside that folder, delete the "openweathermap_service.cfg" since we don't need it anymore, rename the python file to "faker_producer.py". Now let's update each file one by one.

3.1. "another-producer.py": 
- delete line 14-30 and 45-52
- add all from "faker-api/data.py" in
- update the file to make the program run correctly
- remove dataprep package
- here is the complete code for you reference:

```
"""Produce openweathermap content to 'faker' kafka topic."""
import os
import time
from collections import namedtuple
from kafka import KafkaProducer
import json
from mimesis import Science
from mimesis import Development
from mimesis import Code

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TOPIC_NAME = os.environ.get("TOPIC_NAME")
SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 5))

science = Science()
development = Development()
code = Code()

def get_data():
    return {
        "dna_sequence": science.dna_sequence(),
        "rna_sequence": science.rna_sequence(),
        "software_license": development.software_license(),
        "version": development.version(),
        "programming_language": development.programming_language(),
        "os": development.os(),
        "issn": code.issn(),
        "isbn": code.isbn(),
        "ean": code.ean(),
        "pin": code.pin()
    }

def run():
    iterator = 0
    print("Setting up another producer at {}".format(KAFKA_BROKER_URL))
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        # Encode all values as JSON
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    )

    while True:
        sendit = get_data()
        # adding prints for debugging in logs
        print("Sending new another data iteration - {}".format(iterator))
        producer.send(TOPIC_NAME, value=sendit)
        print(sendit)
        print("New another data sent")
        time.sleep(SLEEP_TIME)
        print("Waking up!")
        iterator += 1


if __name__ == "__main__":
    run()

```

3.2. "requirements.txt": 
Replace dataprep with mimesis since we need the faker Python package

3.3. "Dockerfile": 
Rename python file to "another_producer.py"

3.4. "docker-compose.yml":
- update line 4,5, and 9 to another
- update SLEEP_TIME to 5

Now, build the new another producer

```bash
docker-compose -f another-producer/docker-compose.yml up -d
```

```bash
$ docker-compose -f owm-producer/docker-compose.yml up -d     # start the producer that retrieves open weather map
$ docker-compose -f twitter-producer/docker-compose.yml up # start the producer for twitter
```

There is a known issue with reading the tweets and the bug fix will be releashed in Tweetpy 4.0 (details here: https://github.com/tweepy/tweepy/issues/237). Therefore, with the Twitter producer, we will attach the bash to monitor the log to see the magic and retry if the service is stopped. 

## Starting Twitter classifier (plus Weather consumer)

There is another catch: We cannot build the Docker file for the consumer directly with the docker-compose.yml (We can do so with all other yml files, just not this one -.-). So we have to manually go inside the folder "consumers" to build the Docker using command:

```bash
$ docker build -t twitterconsumer .        # start the consumers
```

Then go back up 1 level with "cd .." and we can start consumers:
```bash
$ docker-compose -f consumers/docker-compose.yml up       # start the consumers
```

## Check that data is arriving to Cassandra

First login into Cassandra's container with the following command or open a new CLI from Docker Desktop if you use that.
```bash
$ docker exec -it cassandra bash
```
Once loged in, bring up cqlsh with this command and query twitterdata and weatherreport tables like this:
```bash
$ cqlsh --cqlversion=3.4.4 127.0.0.1 #make sure you use the correct cqlversion

cqlsh> use kafkapipeline; #keyspace name

cqlsh:kafkapipeline> select * from twitterdata;

cqlsh:kafkapipeline> select * from weatherreport;
```

## Visualization

Run the following command the go to http://localhost:8888 and run the visualization notebook accordingly

## Setting up data-vis
Add this to "data-vis/docker-compose.yml"

ANOTHER_TABLE: anotherdata

Copy these in data-vis/python/cassandrautils.py

```
import datetime
import gzip
import os
import re
import sys

import pandas as pd
from cassandra.cluster import BatchStatement, Cluster, ConsistencyLevel
from cassandra.query import dict_factory

tablename = os.getenv("weather.table", "weatherreport")
twittertable = os.getenv("twittertable.table", "twitterdata")
fakertable = os.getenv("fakertable.table", "fakerdata")
anothertable = os.getenv("anothertable.table", "anotherdata")

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST") if os.environ.get("CASSANDRA_HOST") else 'localhost'
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE") if os.environ.get("CASSANDRA_KEYSPACE") else 'kafkapipeline'

WEATHER_TABLE = os.environ.get("WEATHER_TABLE") if os.environ.get("WEATHER_TABLE") else 'weather'
TWITTER_TABLE = os.environ.get("TWITTER_TABLE") if os.environ.get("TWITTER_TABLE") else 'twitter'
FAKER_TABLE = os.environ.get("FAKER_TABLE") if os.environ.get("FAKER_TABLE") else 'faker'
ANOTHER_TABLE = os.environ.get("ANOTHER_TABLE") if os.environ.get("ANOTHER_TABLE") else 'another'

def saveTwitterDf(dfrecords):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    session = cluster.connect(CASSANDRA_KEYSPACE)

    counter = 0
    totalcount = 0

    cqlsentence = "INSERT INTO " + twittertable + " (tweet_date, location, tweet, classification) \
                   VALUES (?, ?, ?, ?)"
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        batch.add(insert, (val['datetime'], val['location'],
                           val['tweet'], val['classification']))
        counter += 1
        if counter >= 100:
            print('inserting ' + str(counter) + ' records')
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter
    rs = [session.execute(b, trace=True) for b in batches]

    print('Inserted ' + str(totalcount) + ' rows in total')


def saveFakerDf(dfrecords):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    session = cluster.connect(CASSANDRA_KEYSPACE)

    counter = 0
    totalcount = 0

    cqlsentence = "INSERT INTO " + fakertable + " (name, address, year, phone, credit_card, company_name, city, job, color, country) \
                   VALUES (?, ?, ?)"
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        batch.add(insert, (val['name'], val['address'], val['year'], val['phone'], val['credit_card'], val['company_name'], val['city'], val['job'], val['color'] ,val['country']))
        counter += 1
        if counter >= 100:
            print('inserting ' + str(counter) + ' records')
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter
    rs = [session.execute(b, trace=True) for b in batches]

    print('Inserted ' + str(totalcount) + ' rows in total')
#AnotherDF
def saveAnotherDf(dfrecords):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    session = cluster.connect(CASSANDRA_KEYSPACE)

    counter = 0
    totalcount = 0

    cqlsentence = "INSERT INTO " + anothertable + " (dna_sequence, rna_sequence, software_license, version, programming_language, os, issn, isbn, ean, pin) \
                   VALUES (?, ?, ?)"
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        batch.add(insert, (val['dna_sequence'], val['rna_sequence'], val['software_license'], val['version'], val['programming_language'], val['os'], val['issn'], val['isbn'], val['ean'] ,val['pin']))
        counter += 1
        if counter >= 100:
            print('inserting ' + str(counter) + ' records')
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter
    rs = [session.execute(b, trace=True) for b in batches]

    print('Inserted ' + str(totalcount) + ' rows in total')

def saveWeatherreport(dfrecords):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    session = cluster.connect(CASSANDRA_KEYSPACE)

    counter = 0
    totalcount = 0

    cqlsentence = "INSERT INTO " + tablename + " (forecastdate, location, description, temp, feels_like, temp_min, temp_max, pressure, humidity, wind, sunrise, sunset) \
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        batch.add(insert, (val['report_time'], val['location'], val['description'],
                           val['temp'], val['feels_like'], val['temp_min'], val['temp_max'],
                           val['pressure'], val['humidity'], val['wind'], val['sunrise'], val['sunset']))
        counter += 1
        if counter >= 100:
            print('inserting ' + str(counter) + ' records')
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter
    rs = [session.execute(b, trace=True) for b in batches]

    print('Inserted ' + str(totalcount) + ' rows in total')


def loadDF(targetfile, target):
    if target == 'weather':
        colsnames = ['description', 'temp', 'feels_like', 'temp_min', 'temp_max',
                     'pressure', 'humidity', 'wind', 'sunrise', 'sunset', 'location', 'report_time']
        dfData = pd.read_csv(targetfile, header=None,
                             parse_dates=True, names=colsnames)
        dfData['report_time'] = pd.to_datetime(dfData['report_time'])
        saveWeatherreport(dfData)
    elif target == 'twitter':
        colsnames = ['tweet', 'datetime', 'location', 'classification']
        dfData = pd.read_csv(targetfile, header=None,
                             parse_dates=True, names=colsnames)
        dfData['datetime'] = pd.to_datetime(dfData['datetime'])
        saveTwitterDf(dfData)
    elif target == 'faker':
        colsnames = ['name', 'address', 'year', 'phone', 'credit_card', 'company_name', 'city', 'job', 'color', 'country']
        dfData = pd.read_csv(targetfile, header=None,
                             parse_dates=True, names=colsnames)
        saveFakerDf(dfData)
    elif target == 'another':
        colsnames = ['dna_sequence', 'rna_sequence', 'software_license', 'version', 'programming_language', 'os', 'issn', 'isbn', 'ean', 'pin']
        dfData = pd.read_csv(targetfile, header=None,
                             parse_dates=True, names=colsnames)
        saveFakerDf(dfData)


def getWeatherDF():
    return getDF(WEATHER_TABLE)
def getTwitterDF():
    return getDF(TWITTER_TABLE)
def getFakerDF():
    print(FAKER_TABLE)
    return getDF(FAKER_TABLE)
def getAnotherkerDF():
    print(ANOTHER_TABLE)
    return getDF(FAKER_TABLE)

def getDF(source_table):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    if source_table not in (WEATHER_TABLE, TWITTER_TABLE, FAKER_TABLE, ANOTHER_TABLE):
        return None

    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.row_factory = dict_factory
    cqlquery = "SELECT * FROM " + source_table + ";"
    rows = session.execute(cqlquery)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    action = sys.argv[1]
    target = sys.argv[2]
    targetfile = sys.argv[3]
    if action == "save":
        loadDF(targetfile, target)
    elif action == "get":
        getDF(target)

```
Build the data-vis
```
docker-compose -f data-vis/docker-compose.yml up -d
```

## Teardown

To stop all running kakfa cluster services

```bash
$ docker-compose -f data-vis/docker-compose.yml down # stop visualization node

$ docker-compose -f consumers/docker-compose.yml down          # stop the consumers

$ docker-compose -f owm-producer/docker-compose.yml down       # stop open weather map producer

$ docker-compose -f twitter-producer/docker-compose.yml down   # stop twitter producer

$ docker-compose -f kafka/docker-compose.yml down              # stop zookeeper, broker, kafka-manager and kafka-connect services

$ docker-compose -f cassandra/docker-compose.yml down          # stop Cassandra
```

To remove the kafka-network network:

```bash
$ docker network rm kafka-network
$ docker network rm cassandra-network
```

To remove resources in Docker

```bash
$ docker container prune # remove stopped containers, done with the docker-compose down
$ docker volume prune # remove all dangling volumes (delete all data from your Kafka and Cassandra)
$ docker image prune -a # remove all images (help with rebuild images)
$ docker builder prune # remove all build cache (you have to pull data again in the next build)
$ docker system prune -a # basically remove everything
```