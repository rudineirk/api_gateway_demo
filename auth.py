#!/usr/bin/env python
import json

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost',
))

QUEUE_NAME = 'rpc.core.auth'

channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME)

def on_request(ch, method, props, body):
    print(" [.] msg = %s" % body)
    try:
        body = json.loads(body.decode()) if body else {}
    except json.decoder.JSONDecodeError:
        return {'status': 'encoding_error'}

    response = {
        'status': 'ok',
        'payload': {'token': 'secret-token-auth'},
    }

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
channel.basic_consume(on_request, queue=QUEUE_NAME)

print(" [x] Awaiting RPC requests")
channel.start_consuming()
