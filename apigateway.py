#!/usr/bin/env python
import json
from urllib.parse import parse_qs
import uuid
from wsgiref.simple_server import make_server

import pika

QUERY_LIMIT = 2500
QUERY_KEYS_LIMIT = 100
PATH_LIMIT = 2500
REQUEST_METHOD_LIMIT = 10
DOMAIN_LIMIT = 2500


SERVICE_MAPPING = {
    ('/api/v1/auth', 'GET'): {
        'type': 'amqp',
        'endpoint': 'rpc.core.auth',
    }
}

class AmqpEndpointClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost',
        ))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, endpoint, data):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=endpoint,
            properties=pika.BasicProperties(
                reply_to = self.callback_queue,
                correlation_id = self.corr_id,
            ),
            body=data
        )

        while self.response is None:
            self.connection.process_data_events()
        return self.response

def get_query(query_string):
    if not query_string:
        return {}

    tmp_query = parse_qs(query_string)
    query = {}
    for key in list(tmp_query)[:QUERY_KEYS_LIMIT]:
        if len(tmp_query[key]) <= 1:
            query[key] = tmp_query[key][0]

    return query


def get_body(content_length, wsgi_input):
    try:
        request_body_size = int(content_length)
    except (ValueError):
        request_body_size = 0

    if request_body_size <= 0:
        return ''

    return wsgi_input.read(request_body_size)


def query_amqp_endpoint(endpoint, data):
    client = AmqpEndpointClient()
    return json.loads(client.call(endpoint, data).decode())


def app(env, resp):
    query_string = env.get('QUERY_STRING', '')[:QUERY_LIMIT]
    query = get_query(query_string)
    body = get_body(env.get('CONTENT_LENGTH', 0), env['wsgi.input'])

    method = env.get('REQUEST_METHOD', 'GET')[:REQUEST_METHOD_LIMIT]
    # domain = env.get('HTTP_HOST', '')[:DOMAIN_LIMIT]
    path = env.get('PATH_INFO', '')[:PATH_LIMIT]
    path = path.rstrip('/')

    try:
        service = SERVICE_MAPPING[(path, method)]
    except KeyError:
        status = '404 Not Found'
        response_headers = [('Content-type', 'text/plain')]
        resp(status, response_headers)
        return []

    try:
        body = json.loads(body) if body else {}
    except json.decoder.JSONDecodeError:
        status = '400 Bad Request'
        response_headers = [('Content-type', 'text/plain')]
        resp(status, response_headers)
        return []

    if service['type'] == 'amqp':
        ret = query_amqp_endpoint(
            service['endpoint'],
            json.dumps({
                'user_id': 'id-number',
                'payload': body if body else query
            })
        )
    else:
        status = '404 Not Found'
        response_headers = [('Content-type', 'text/plain')]
        resp(status, response_headers)
        return []

    if ret['status'] == 'ok':
        status = '200 OK'
        response_headers = [('Content-type', 'application/json')]
        data = [json.dumps(ret['payload']).encode()]
    elif ret['status'] == 'encoding_error':
        status = '400 Bad Request'
        response_headers = [('Content-type', 'text/plain')]
        data = []
    else:
        status = '500 Internal Server Error'
        response_headers = [('Content-type', 'text/plain')]
        data = []

    resp(status, response_headers)
    return data


    resp(status, response_headers)
    return data


if __name__ == '__main__':
    httpd = make_server('localhost', 5000, app)
    print('Starting server on http://localhost:5000')
    httpd.serve_forever()
