import pika
import json

output_type = input('Please enter desirable output type XML/TABLE/CSV/JSON: ').upper()

while output_type not in ('JSON', 'XML', 'CSV', 'TABLE'):
    output_type = input('oops! please enter one of the following: XML/TABLE/CSV/JSON: ').upper()

sqlite_dblink = input('Please enter path to chinook db like - C:\sqlite\chinook.db: ')

msg = {'link': sqlite_dblink,
       'type': output_type}

data_out = json.dumps(msg)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=data_out)

print("[X] Sent 'JSON file'")
connection.close()


