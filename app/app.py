from multiprocessing import Process
from snowplow_tracker import Subject, Tracker, AsyncEmitter
from snowplow_tracker import SelfDescribingJson
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from jsonschema import validate
from datetime import datetime
from time import sleep
import jsonschema
import threading
import psycopg2
from psycopg2 import pool
import logging
import urllib
import signal
import sys
import json
import os
import ssl

# set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler for logs at the INFO level
# This will be emailed when the cron task runs; formatted to give messages only
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s: %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# create file handler for logs at the DEBUG level
log_filename = '{0}'.format(os.path.basename(__file__).replace('.py', '.log'))
log_handler = logging.FileHandler(
    log_filename, "a", encoding=None, delay="true")
log_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(levelname)s:%(name)s:%(asctime)s:%(message)s")
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)


def signal_handler(signal, frame):
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Retrieve env variables
host = os.getenv('DB_HOSTNAME')
database = os.getenv('DB_NAME')
user = os.getenv('DB_USERNAME')
cert_path = os.getenv('SERVING_CERT_PATH')
password = os.getenv('DB_PASSWORD')

address = '0.0.0.0'
port = 8443

# set up dicts to track emitters and trackers
e = {}
t = {}

# Database Query Strings
# Timestamps are in ms and their calculation for insertion as a datetime is handled by postgres, which natively regards datetimes as being in seconds.
client_calls_sql = """INSERT INTO caps.client_calls(received_timestamp,ip_address,response_code,raw_data,environment,namespace,app_id,device_created_timestamp,event_data_json)
                      VALUES(NOW(), %s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s::decimal/1000), %s) RETURNING request_id ;"""

snowplow_calls_sql = """INSERT INTO caps.snowplow_calls(request_id, sent_timestamp, snowplow_response, try_number,environment,namespace,app_id,device_created_timestamp,event_data_json)
                        VALUES(%s, NOW(), %s, %s, %s, %s, %s, TO_TIMESTAMP(%s::decimal/1000), %s) RETURNING snowplow_send_id ;"""

# POST body JSON validation schema
post_schema = json.load(open('post_schema.json', 'r'))


# Use getconn() method to Get Connection from connection pool
# Returns the value of the generated identifier (index)
def single_response_query(sql, execute_tuple, all=False):
    conn = None
    fetch = None
    try:
        conn = threaded_postgreSQL_pool.getconn()
        if(conn):
            cur = conn.cursor()
            cur.execute(sql, execute_tuple)
            if all:
                fetch = cur.fetchall()
            else:
                fetch = cur.fetchone()
            conn.commit()
            cur.close()
    except (Exception, psycopg2.DatabaseError) as e:
        logger.exception("Error retreiving from connection pool")
    finally:
        if conn is not None:
            # Release the connection object and send it back to the pool
            threaded_postgreSQL_pool.putconn(conn)
    return fetch

# Used for on_failure retry backoff; returns the number at index n in the Fibonacci sequence
# def binets_formula(n):
#     sqrt5 = 5 ** 0.5
#     F_n = int((( (1 + sqrt5) ** n - (1 - sqrt5) ** n ) / ( 2 ** n * sqrt5 )))
#     return F_n

def call_snowplow(request_id, json_object):

    # Use the global emitter and tracker dicts
    global e
    global t

    # callbacks are documented in
    # - https://github.com/snowplow/snowplow/wiki/Python-Tracker#emitters

    # callback for passed calls
    def on_success(successfully_sent_count):
        logger.info("Emitter call PASSED on request_id: {}.".format(request_id))
        backoff_outside_the_scope = 1 # reset the backoff if this is successful
        # get previous try number, choose larger of 0 or query result and add 1
        try_number = max(i for i in [0,single_response_query("SELECT MAX(try_number) FROM caps.snowplow_calls WHERE request_id = %s ;", (request_id, ))[0]] if i is not None) + 1
        logger.debug("Try number: {}".format(try_number))
        snowplow_tuple = (
            str(request_id),
            str(200),
            str(try_number),
            json_object['env'],
            json_object['namespace'],
            json_object['app_id'],
            json_object['dvce_created_tstamp'],
            json.dumps(json_object['event_data_json'])
        )
        snowplow_id = single_response_query(snowplow_calls_sql, snowplow_tuple)[0]
        logger.info("snowplow call table insertion PASSED on request_id: {} and snowplow_id: {}.".format(request_id, snowplow_id))
        return

    # callback for failed calls
    failed_try = 0
    def on_failure(successfully_sent_count, failed_events):
        # increment the failed try
        nonlocal failed_try
        failed_try += 1

        # sleep according to the number indexed by failed_try in the fibonacci sequence
        # sleep_time = binets_formula(failed_try)
        logger.info("Emitter call FAILED on request_id {} on try {}. No re-attempt will be made.".format(request_id,failed_try))

        # Leaving this sleep delay until inputting after a failed event is ready
        #sleep(sleep_time)

        # failed_events should always contain only one event, because ASyncEmitter has a buffer size of 1
        for event in failed_events:
            # get previous try number, choose larger of 0 or query result and add 1
            # try_number = max(i for i in [0,single_response_query("SELECT MAX(try_number) FROM caps.snowplow_calls WHERE request_id = %s ;", (request_id, ))[0]] if i is not None) + 1
            # log("DEBUG","Try number: {}".format(try_number))
            snowplow_tuple = (
                str(request_id),
                str(400),
                str(failed_try),
                json_object['env'],
                json_object['namespace'],
                json_object['app_id'],
                json_object['dvce_created_tstamp'],
                json.dumps(json_object['event_data_json'])
            )
            snowplow_id = single_response_query(snowplow_calls_sql, snowplow_tuple)[0]
            logger.info("snowplow call table insertion PASSED on request_id: {} and snowplow_id: {}.".format(request_id, snowplow_id))
            # Re-attempt the event call by inputting it back to the emitter
            #e[tracker_identifier].input(event)

    tracker_identifier = json_object['env'] + "-" + json_object['namespace'] + "-" + json_object['app_id']
    logger.debug("New request with tracker_identifier {}".format(tracker_identifier))

    # logic to switch between SPM and Production Snowplow.
    # TODO: Fix SSL problem so to omit the anonymization proxy, since we connect from a Gov IP, not a personal machine
    sp_endpoint = os.getenv("SP_ENDPOINT_{}".format(json_object['env'].upper()))
    logger.debug("Using Snowplow Endpoint {}".format(sp_endpoint))

    # Set up the emitter and tracker. If there is already one for this combination of env, namespace, and app-id, reuse it
    # TODO: add error checking
    if tracker_identifier not in e:
        # defaults to a GET method, defaults to a buffer size of 1; buffer is flushed once full.
        e[tracker_identifier] = AsyncEmitter(sp_endpoint, protocol="https", on_success=on_success, on_failure=on_failure)
    if tracker_identifier not in t:
        t[tracker_identifier] = Tracker(e[tracker_identifier], encode_base64=False, app_id=json_object['app_id'], namespace=json_object['namespace'])

    # Build event JSON
    # TODO: add error checking
    event = SelfDescribingJson(json_object['event_data_json']['schema'], json_object['event_data_json']['data'])
    # Build contexts
    # TODO: add error checking
    contexts = []
    for context in json_object['event_data_json']['contexts']:
        contexts.append(SelfDescribingJson(context['schema'], context['data']))

    # Send call to Snowplow
    # TODO: add error checking
    t[tracker_identifier].track_self_describing_event(event, contexts, tstamp=json_object['dvce_created_tstamp'])


class RequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        logger.info("got POST request")
        ip_address = self.client_address[0]
        headers = self.headers
        length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(length).decode('utf-8')

        logger.info("IP: {}".format(ip_address))
        logger.info("HEADERS: {}".format(self.headers))

        # Test that the post_data is JSON
        try:
            json_object = json.loads(post_data)
        except (json.decoder.JSONDecodeError, ValueError):
            response_code = 400
            try:
                self.send_response(response_code, 'POST body is not parsable as JSON.')
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
            except ConnectionResetError:
                pass
            post_tuple = (ip_address,response_code,post_data,None,None,None,None,None)
            request_id = single_response_query(client_calls_sql,post_tuple)[0]
            return

        # Test that the JSON matches the expeceted schema
        try:
            jsonschema.validate(json_object, post_schema)
        except (jsonschema.ValidationError, jsonschema.SchemaError):
            response_code = 400
            try:
                self.send_response(response_code, 'POST JSON is not compliant with schema.')
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
            except ConnectionResetError:
                pass
            post_tuple = (ip_address,response_code,post_data,None,None,None,None,None)
            request_id = single_response_query(client_calls_sql,post_tuple)[0]
            return

        # Test that the input device_created_timestamp is in ms
        device_created_timestamp = json_object['dvce_created_tstamp']
        if device_created_timestamp < 99999999999: # 11 digits, Sat Mar 03 1973 09:46:39 UTC
            # it's too small to be in seconds
            response_code = 400
            try:
                self.send_response(response_code, 'Device Created Timestamp is not in milliseconds.')
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
            except ConnectionResetError:
                pass
            post_tuple = (ip_address,response_code,post_data,None,None,None,None,None)
            request_id = single_response_query(client_calls_sql,post_tuple)[0]
            return

        # Input POST is JSON and validates to schema
        response_code = 200
        try:
            self.send_response(response_code)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
        except ConnectionResetError:
            pass

        # insert the parsed post body into the client_calls table
        post_tuple = (
            ip_address,
            response_code,
            post_data,
            json_object['env'],
            json_object['namespace'],
            json_object['app_id'],
            json_object['dvce_created_tstamp'],
            json.dumps(json_object['event_data_json'])
            )
        request_id = single_response_query(client_calls_sql, post_tuple)[0]

        logger.info("Issue Snowplow call: Request ID {}.".format(request_id))
        call_snowplow(request_id, json_object)
        return

# if single_response_query("SELECT 1 ;",None)[0] is not 1:
#     log("ERROR","There is a problem querying the database.")
#     sys.exit(1)


# ThreadedHTTPServer reference:
# - https://pymotw.com/2/BaseHTTPServer/index.html#module-BaseHTTPServer
# - https://stackoverflow.com/a/36439055/5431461
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    def server_activate(self):
        self.socket.listen(20)


print("\nGDX Analytics as a Service\n===")

# Create a threaded PostgreSQL connection pool
try:
    threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=5,
        maxconn=20,
        user=user,
        password=password,
        host=host,
        database=database)
    if(threaded_postgreSQL_pool):
        logger.info("Connection pool created successfully")
except (Exception, psycopg2.DatabaseError) as e:
    logger.exception("Error while connecting to PostgreSQL")

httpd = ThreadedHTTPServer((address, port), RequestHandler)
logger.info("Listening for TCP requests to {} on port {}.".format(address, port))
httpd.socket = ssl.wrap_socket(
    httpd.socket,
    keyfile="{cert_path}/tls.key".format(cert_path=cert_path),
    certfile='{cert_path}/tls.crt'.format(cert_path=cert_path),
    server_side=True)
httpd.serve_forever()
