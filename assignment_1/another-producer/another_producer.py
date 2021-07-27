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