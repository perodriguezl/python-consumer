#!/usr/bin/env python
import json
import logging
import time

import pika
import requests

logging.basicConfig(level=logging.DEBUG, filename="logfile", filemode="a+", format="%(asctime)-15s %(levelname)-8s %(message)s")



connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
result = channel.queue_declare(exclusive=True)
queue_name = "external_notification"
channel.queue_bind(exchange='direct_logs', queue=queue_name)
print(' [*] Waiting for logs. To exit press CTRL+C')

def onMessage(ch, method, properties, body):
    while (True):
        try:
            r = requests.post('http://localhost:8080/tracking/login', data=body)
            response = json.loads(r.text)
            if ('error' in response):
                logging.error(response)
                raise Exception(response['error'])
            logging.info('Successfully Processed!')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            break
        except Exception as inst:
            logging.info('Waiting 10 seconds to retry')
            time.sleep(10)
            continue


channel.basic_consume(onMessage, queue=queue_name)

channel.start_consuming()