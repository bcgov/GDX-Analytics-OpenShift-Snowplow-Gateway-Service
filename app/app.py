'''CAPS Analytics as a Service on OpenShift'''
import logging
import signal
import sys
import json
import os
from socketserver import ThreadingMixIn
from http.server import HTTPServer, BaseHTTPRequestHandler
from snowplow_tracker import Tracker, AsyncEmitter
from snowplow_tracker import SelfDescribingJson
import jsonschema
import psycopg2
from psycopg2 import pool

# set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler for logs at the DEBUG level
# This will be emailed when the cron task runs; formatted to give messages only
stream_handler = logging.StreamHandler()
formatter = logging.Formatter(":%(name)s:%(asctime)s:%(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def signal_handler(signal, frame):
    '''suppressing signal.signal signal_handler function with this override'''
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
# Timestamps are in ms and their calculation for insertion as a datetime is
# handled by postgres, which natively regards datetimes as being in seconds.
client_calls_sql = """
INSERT INTO caps.client_calls(
  received_timestamp,
  ip_address,
  response_code,
  raw_data,
  environment,
  namespace,
  app_id,
  device_created_timestamp,
  event_data_json)
VALUES(NOW(), %s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s::decimal/1000), %s)
RETURNING request_id ;
"""

snowplow_calls_sql = """
INSERT INTO caps.snowplow_calls(
  request_id,
  sent_timestamp,
  snowplow_response,
  try_number,
  environment,
  namespace,
  app_id,
  device_created_timestamp,
  event_data_json)
VALUES(%s, NOW(), %s, %s, %s, %s, %s, TO_TIMESTAMP(%s::decimal/1000), %s)
RETURNING snowplow_send_id ;
"""

# POST body JSON validation schema
post_schema = json.load(open('post_schema.json', 'r'))


# Use getconn() method to Get Connection from connection pool
# Returns the value of the generated identifier (index)
def single_response_query(sql, execute_tuple, fetch_all=False):
    '''Run a query on the database an return a single falue response'''
    conn = None
    fetch = None
    try:
        conn = threaded_postgreSQL_pool.getconn()
        if conn:
            while True:
                try:
                    cur = conn.cursor()
                    cur.execute(sql, execute_tuple)
                # An OperationalError suggests that the connection broke.
                # In those instances, we will re-create a single connection.
                except psycopg2.OperationalError:
                    logger.exception("OperationalError. Return this "
                                     "connection to the pool "
                                     "as a closed connection, "
                                     "and get a new one.")
                    threaded_postgreSQL_pool.putconn(conn, close=True)
                    conn = threaded_postgreSQL_pool.getconn()
                else:
                    break
            if fetch_all:
                fetch = cur.fetchall()
            else:
                fetch = cur.fetchone()
            conn.commit()
            cur.close()
    except psycopg2.DatabaseError:
        logger.exception("Error retreiving from connection pool")
    finally:
        if conn is not None:
            # Release the connection object and send it back to the pool
            threaded_postgreSQL_pool.putconn(conn)
    return fetch


def call_snowplow(request_id, json_object):
    '''Callback executed when an emitter is flushed successfully'''
    # Debugging request_id to see if it's being evaluated by the callbacks
    logger.info("Request ID on call_snowplow function: %s", request_id)

    # Use the global emitter and tracker dicts
    global e
    global t

    def callback_log_inscope():
        logger.info("callback_log_inscope has Request ID: %s", request_id)

    # callbacks are documented in
    # - https://github.com/snowplow/snowplow/wiki/Python-Tracker#emitters

    # callback for passed calls
    def on_success(successfully_sent_count):
        logger.info('\'on_success\' callback with %s successful events',
                    successfully_sent_count)
        callback_log_inscope()
        logger.info(
            "Emitter call PASSED on request_id: %s.", request_id)
        # get previous try number, choose larger of 0 or query result and add 1
        max_try_number_query = (
            "SELECT MAX(try_number) "
            "FROM caps.snowplow_calls "
            "WHERE request_id = %s ;")
        try_number = max(
            i for i in [0, single_response_query(
                max_try_number_query, (request_id, ))[0]] if i is not None) + 1
        logger.debug("Try number: %s", try_number)
        snowplow_tuple = (
            str(request_id),
            str(200),
            str(try_number),
            json_object['env'],
            json_object['namespace'],
            json_object['app_id'],
            json_object['dvce_created_tstamp'],
            json.dumps(json_object['event_data_json']))
        snowplow_id = single_response_query(
            snowplow_calls_sql, snowplow_tuple)[0]
        logger.info("snowplow call table insertion PASSED on "
                    "request_id: %s and snowplow_id: %s.",
                    request_id, snowplow_id)

    # callback for failed calls
    failed_try = 0

    def on_failure(successfully_sent_count, failed_events):
        '''Callback executed when an emitter flush results in any failures'''
        # increment the failed try
        logger.warning('\'on_failure\' callback: %s events successfully '
                       'emitted, %s events returned by emitter with an error '
                       'response', successfully_sent_count, len(failed_events))
        nonlocal failed_try
        failed_try += 1

        logger.info(
            'Emitter call FAILED on request_id %s on try %s. '
            'No re-attempt will be made.', request_id, failed_try)

        # failed_events should always contain only one event,
        # because ASyncEmitter has a buffer size of 1
        for event in failed_events:
            logger.warning('event failure: %s', event)
            snowplow_tuple = (
                str(request_id), str(400), str(failed_try),
                json_object['env'], json_object['namespace'],
                json_object['app_id'], json_object['dvce_created_tstamp'],
                json.dumps(json_object['event_data_json']))
            snowplow_id = single_response_query(
                snowplow_calls_sql, snowplow_tuple)[0]
            logger.info("snowplow call table insertion PASSED on request_id: "
                        "%s and snowplow_id: %s.", request_id, snowplow_id)
            # Re-attempt the event call by inputting it back to the emitter

    tracker_identifier = "{}-{}-{}".format(
        json_object['env'], json_object['namespace'], json_object['app_id'])
    logger.debug("New request with tracker_identifier %s", tracker_identifier)

    # logic to switch between SPM and Production Snowplow.
    sp_route = os.getenv("SP_ENDPOINT_{}".format(json_object['env'].upper()))
    logger.debug("Using Snowplow Endpoint %s", sp_route)

    # Set up the emitter and tracker. If there is already one for this
    # combination of env, namespace, and app-id, reuse it
    # TODO: add error checking
    # TEMP COMMENTED OUT TO AVOID USING THE GLOBAL DICT OF EMITTERS/TRACKERS
    # if tracker_identifier not in e:
    #     e[tracker_identifier] = AsyncEmitter(
    #         sp_route,
    #         protocol="https",
    #         on_success=on_success,
    #         on_failure=on_failure)
    #
    # if tracker_identifier not in t:
    #     t[tracker_identifier] = Tracker(
    #         e[tracker_identifier],
    #         encode_base64=False,
    #         app_id=json_object['app_id'],
    #         namespace=json_object['namespace'])

    this_ASyncEmitter = AsyncEmitter(sp_route,
                                     protocol="https",
                                     on_success=on_success,
                                     on_failure=on_failure)
    this_Tracker = Tracker(this_ASyncEmitter,
                           encode_base64=False,
                           app_id=json_object['app_id'],
                           namespace=json_object['namespace'])

    # Build event JSON
    # TODO: add error checking
    event = SelfDescribingJson(
        json_object['event_data_json']['schema'],
        json_object['event_data_json']['data'])
    # Build contexts
    # TODO: add error checking
    contexts = []
    for context in json_object['event_data_json']['contexts']:
        contexts.append(SelfDescribingJson(context['schema'], context['data']))

    # Send call to Snowplow
    # TODO: add error checking
    # TEMP COMMENTED OUT TO AVOID USING THE GLOBAL DICT OF EMITTERS/TRACKERS
    # t[tracker_identifier].track_self_describing_event(
    #     event, contexts, tstamp=json_object['dvce_created_tstamp'])

    this_Tracker.track_self_describing_event(
        event, contexts, tstamp=json_object['dvce_created_tstamp']
    )


class RequestHandler(BaseHTTPRequestHandler):
    '''This is the Request Handler running on the multithreaded HTTPServer'''

    # Necessary for connection persistence (keepalive)
    protocol_version = 'HTTP/1.1'

    # Suppress the stderr log output from BaseHTTPRequestHandler
    # https://stackoverflow.com/questions/3389305/how-to-silent-quiet-httpserver-and-basichttprequesthandlers-stderr-output
    def log_message(self, format, *args):
        return

    # suppress ConnectionResetErrors because we cannot identify the origin
    # https://chromium.googlesource.com/external/trace-viewer/+/bf55211014397cf0ebcd9e7090de1c4f84fc3ac0/third_party/Paste/paste/httpserver.py
    def handle(self):
        try:
            BaseHTTPRequestHandler.handle(self)
        except ConnectionResetError:
            # logger.exception("There was a ConnectionResetError: ")
            pass

    # a GET method is implemented for container health checks
    def do_GET(self):
        '''Respond to a GET request. Use GET for liveness checks or testing.'''
        # suppress logging kube-probe (the kubernetes health check user-agent)
        if 'kube-probe' not in self.headers['User-Agent']:
            logger.info("GET request,\nPath: %s\nHeaders:\n%s\n",
                        str(self.path), str(self.headers))
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    # a POST method handles the JSON requests intended for Snowplow
    def do_POST(self):
        '''Respond to a POST request. Clients use POST to deliver events.'''
        logger.info("got POST request")
        ip_address = self.client_address[0]
        # headers = self.headers
        length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(length).decode('utf-8')

        logger.info("IP: %s", ip_address)
        logger.info("HEADERS: %s", self.headers)

        # Test that the post_data is JSON
        try:
            json_object = json.loads(post_data)
        except (json.decoder.JSONDecodeError, ValueError):
            response_code = 400
            try:
                self.send_response(response_code,
                                   'POST body is not parsable as JSON.')
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
            except ConnectionResetError:
                pass
            post_tuple = (ip_address, response_code, post_data,
                          None, None, None, None, None)
            request_id = single_response_query(client_calls_sql, post_tuple)[0]
            return

        # Test that the JSON matches the expeceted schema
        try:
            jsonschema.validate(json_object, post_schema)
        except (jsonschema.ValidationError, jsonschema.SchemaError):
            response_code = 400
            try:
                self.send_response(
                    response_code, 'POST JSON is not compliant with schema.')
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
            except ConnectionResetError:
                pass
            post_tuple = (ip_address, response_code, post_data,
                          None, None, None, None, None)
            request_id = single_response_query(client_calls_sql, post_tuple)[0]
            return

        # Test that the input device_created_timestamp is in ms
        device_created_timestamp = json_object['dvce_created_tstamp']
        # if the device_created_timestamp is greater than 11 digits,
        # then it is older than Sat Mar 03 1973 09:46:39 UTC
        # which means it has been provided in milliseconds
        if device_created_timestamp < 99999999999:
            # it's too small to be in seconds
            response_code = 400
            try:
                self.send_response(
                    response_code,
                    'Device Created Timestamp is not in milliseconds.')
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
            except ConnectionResetError:
                pass
            post_tuple = (ip_address, response_code, post_data,
                          None, None, None, None, None)
            request_id = single_response_query(client_calls_sql, post_tuple)[0]
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
        post_tuple = (ip_address, response_code, post_data,
                      json_object['env'], json_object['namespace'],
                      json_object['app_id'],
                      json_object['dvce_created_tstamp'],
                      json.dumps(json_object['event_data_json']))
        request_id = single_response_query(client_calls_sql, post_tuple)[0]

        # Explicitly finishes this request. Resolves cURL hangs while testing.
        # self.finish()

        logger.info("Issue Snowplow call: Request ID %s.", request_id)
        call_snowplow(request_id, json_object)
        return


# ThreadedHTTPServer reference:
# - https://pymotw.com/2/BaseHTTPServer/index.html#module-BaseHTTPServer
# - https://stackoverflow.com/a/36439055/5431461
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    '''This allows us to use a multithreaded HTTPServer'''
    #
    # def server_activate(self):
    #     self.socket.listen(128)


print("\nGDX Analytics as a Service\n===")

# Create a threaded PostgreSQL connection pool
try:
    threaded_postgreSQL_pool = pool.ThreadedConnectionPool(
        minconn=5,
        maxconn=100,
        user=user,
        password=password,
        host=host,
        database=database)
    if threaded_postgreSQL_pool:
        logger.info("Connection pool created successfully")
except psycopg2.DatabaseError:
    logger.exception("Error while connecting to PostgreSQL")

httpd = ThreadedHTTPServer((address, port), RequestHandler)
logger.info("Listening for TCP requests to %s on port %s.", address, port)
httpd.serve_forever()
