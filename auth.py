#!/usr/bin/env python
import json

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost',
))

channel = connection.channel()
channel.queue_declare(queue='core.auth')

def on_request(ch, method, props, body):
    print(" [.] msg = %s" % body)
    response = {'token': 'secret-token-auth'}

    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id = props.correlation_id
        ),
        body=json.dumps(response),
    )
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='core.auth')

print(" [x] Awaiting RPC requests")
channel.start_consuming()
