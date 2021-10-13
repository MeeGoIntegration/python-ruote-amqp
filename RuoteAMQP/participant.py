# Copyright (C) 2010 Nokia Corporation and/or its subsidiary(-ies).
# Contact: David Greaves <ext-david.greaves@nokia.com>
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

""" Abstract participant class """

import sys
import traceback
from threading import Thread
import functools
from urllib.error import HTTPError
import pika
from RuoteAMQP.workitem import Workitem
import logging
import time

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s: %(message)s',
                    level=logging.INFO)

try:
    import json
except ImportError:
    import simplejson as json


def format_ruby_backtrace(trace):
    """Formats a python traceback so that a ruby Exception accepts it
       as a backtrace."""
    return ["%s:%d: in `%s %s'" % (item[0], item[1], item[2], item[3])
            for item in trace]


def format_exception(exc):
    """Formats exception to more informative string based on exception type."""
    if isinstance(exc, HTTPError):
        # Python bug, HTTPError does not always have url attribute and geturl()
        # fails
        exc_str = "HTTPError: %d %s" % (exc.getcode(), exc.filename)
    # Catching EnvironmentError means we cover IOError, OSError, URLError
    # http://docs.python.org/library/exceptions.html#exceptions.EnvironmentError
    elif isinstance(exc, EnvironmentError):
        if exc.filename:
            exc_str = "{0}({1}): {2} {3}".format(exc.__class__.__name__,
                                                 exc.errno, exc.filename,
                                                 exc.strerror)
        elif exc.errno and exc.strerror:
            exc_str = "{0}({1}): {2}".format(exc.__class__.__name__,
                                             exc.errno, exc.strerror)
        else:
            exc_str = "{0}: {1}".format(exc.__class__.__name__, str(exc))
    # osc exceptions don't set args and message correctly so str(exc) contains
    # only the exception class name. However it has a msg attribute which has
    # sensible contents so use that
    elif hasattr(exc, "msg"):
        exc_str = "{0}: {1}".format(exc.__class__.__name__, exc.msg)
    else:
        exc_str = "{0}: {1}".format(exc.__class__.__name__, str(exc))
    return exc_str


def format_block(msg):
    """Format message in a block with separator lines at begining and end."""
    return "\n%s\n%s\n%s\n" % ("-" * 78,  msg, "-" * 78)


class ConsumerThread(Thread):
    """Thread for running the Participant.consume(wi)"""
    def __init__(self, participant, delivery_tag, workitem):
        """
        Pass in the participant and the tag for the message
        this thread is handling
        """
        super(ConsumerThread, self).__init__()
        self.__participant = participant
        self.delivery_tag = delivery_tag
        self.workitem = workitem
        self.exception = None
        self.trace = None
        self.log = logging.getLogger(__name__)

    def run(self):
        try:
            # Run the actual code for the workitem
            self.__participant.consume(self.workitem)
        except Exception as exobj:
            # This should be configureable:
            self.log.error("Exception in participant %s\n"
                           "while handling instance %s of process %s\n"
                           "Note: for information only. Participant remains"
                           " functional.\n"
                           "Error is being signalled to the workflow (unless"
                           " this workitem is 'forgotten').\n" %
                           (self.workitem.participant_name,
                            self.workitem.wfid,
                            self.workitem.wf_name))
            self.log.error(format_block(traceback.format_exc()))
            self.exception = exobj
            self.trace = traceback.extract_tb(sys.exc_info()[2])

            self.workitem.error = {
                # This should be BOSS:RemoteError but BOSS got reverted
                # "class": "BOSS::RemoteError",
                "class": "BOSS::StandardError",
                "message": format_exception(self.exception),
                "trace": format_ruby_backtrace(self.trace)}

        # Acknowledge the message as received *after* it has been processed
        self.__participant.ack_message_from_a_thread(self.delivery_tag)

        if not self.workitem.forget:
            self.__participant.reply_to_engine_from_thread(self.workitem)


class Participant:
    """
    A Participant will do work in a Ruote process. Participant is
    essentially abstract and must be subclassed to provide a useful
    consume(wi) method.

    Workitems arrive via AMQP, are processed and returned to the Ruote engine.

    Cancel is not yet implemented.
    """
    # Threaded pika consumer
    # https://github.com/pika/pika/blob/master/examples/basic_consumer_threaded.py
    def __init__(self, ruote_queue,
                 amqp_host="localhost", amqp_user="ruote",
                 amqp_pass="ruote", amqp_vhost="ruote"):

        port = 5672
        if ":" in amqp_host:
            (amqp_host, port) = amqp_host.split(":")
        self._conn_params = dict(
            host=amqp_host,
            port=port,
            virtual_host=amqp_vhost,
            heartbeat=5,
            credentials=pika.PlainCredentials(amqp_user, amqp_pass)
            )
        self._connection = None
        self._channel = None
        # If we reconnect then use the same channel so an ack can be sent back
        self._channel_number = None
        self._queue = ruote_queue
        self._consumer_tag = None
        self._running = False
        self.log = logging.getLogger(__name__)
        self.log.info("params=%s" % self._conn_params)

    def consume(self, workitem):
        """
        Override the consume(wi) method in a subclass to do useful work.
        The workitem is passed back by the worker thread.
        """
        pass

    def cancel(self, workitem):
        """
        Override the cancel(workitem) method in a subclass to respond to a
        cancel message.
        """
        self.log.warning("Ignoring a cancel message")

    def stop(self):
        """
        Override the stop() method in a subclass to cleanly shutdown
        """
        self.log.warning("Exiting via default Participant.stop()")

    # Handle the message
    def workitem_callback(self, chan, method, properties, body):
        """
        This is where a workitem message is handled
        channel: pika.Channel
        method: pika.spec.Basic.Deliver
        properties: pika.spec.BasicProperties
        body: bytes
        """
        tag = method.delivery_tag
        self._consumer_tag = method.consumer_tag
        try:
            workitem = Workitem(body)
        except ValueError as exobj:
            # Reject and don't requeue the message
            chan.basic_reject(delivery_tag=tag, requeue=False)
            self.log.error("Exception decoding incoming json\n"
                           "%s\n"
                           "Note: Now re-raising exception\n" %
                           format_block(body))
            raise exobj

        # Launch consume(wi) in separate thread so it doesn't get
        # interrupted by signals and allows the connection to keep
        # heartbeating
        if not workitem.is_cancel:
            self.consumer = ConsumerThread(self, tag, workitem)
            self.consumer.start()
            self.log.info("Consumer running")
        else:
            self.cancel(workitem)
            self._ack_message_cb(tag)

    # This method is called from the worker thread
    def ack_message_from_a_thread(self, delivery_tag):
        if not (self._connection and self._connection.is_open):
            self.log.error("Connection has gone away"
                           "- message cannot be ack'ed :(")
        else:
            cb = functools.partial(self._ack_message_cb, delivery_tag)
            self._connection.add_callback_threadsafe(cb)

    # This method is called by the pika thread
    def _ack_message_cb(self, delivery_tag):
        try:
            self._channel.basic_ack(delivery_tag)
        except pika.ChannelClosed:
            self.log.error("Channel was closed. message cannot be ack'ed :(")

    def reply_to_engine_from_thread(self, workitem):
        """
        When the job is complete the workitem is passed back to the
        ruote engine.  The consume() method should set the
        workitem.result() if required.
        """
        msg = json.dumps(workitem.to_h())

        # Get the pika thread to send the msg
        cb = functools.partial(self._reply_to_engine_cb, msg)
        while not (self._connection and self._connection.is_open):
            self.log.debug("Waiting for connection to recover"
                           "to reply to engine")
            time.sleep(5)
        self._connection.add_callback_threadsafe(cb)

    def _reply_to_engine_cb(self, msg):
        while not (self._channel and self._channel.is_open):
            self.log.debug("Waiting for channel to recover to"
                           " reply to engine")
            time.sleep(5)

        # delivery_mode=2 is persistent
        props = pika.BasicProperties(content_type='text/plain',
                                     delivery_mode=2)

        # Publish the message.
        # Notice that this is sent to the anonymous/'' exchange (which is
        # different to 'amq.direct') with a routing_key for the queue
        self._channel.basic_publish(exchange='',
                                    routing_key='ruote_workitems',
                                    body=msg,
                                    properties=props)

    def run(self):
        """
        Currently an infinite loop waiting for messages on the AMQP channel.
        """
        if self._running:
            raise RuntimeError("Participant already running")

        self._running = True
        while self._running:
            try:
                self._connection = pika.BlockingConnection(
                    pika.ConnectionParameters(**self._conn_params))
                # _channel_number is None first time - then we
                # remember and use the assigned value on subsequent
                # calls
                self._channel = self._connection.channel(self._channel_number)
                self._channel_number = self._channel.channel_number
                # set qos option on this channel with prefetch count 1 whole
                # message of any size.

                # prefetch_count essentially controls the
                # 'threadedness' of the participant
                self._channel.basic_qos(prefetch_size=0,
                                        prefetch_count=1)
                self._channel.queue_declare(
                    queue=self._queue, durable=True, exclusive=False,
                    auto_delete=False)

                # Listen on the queue associated with this participant
                # Don't auto_ack - wait until the msg is actually
                # processed
                self._channel.basic_consume(
                    queue=self._queue,
                    auto_ack=False,
                    on_message_callback=self.workitem_callback)

                try:
                    self._channel.start_consuming()
                except pika.exceptions.ConnectionClosed:
                    self.log.warning('Connection was closed. Recovering in 5s')
                    self._channel = None  # Ensure we can't accidentally use it
                    time.sleep(5)
                except KeyboardInterrupt:
                    self._channel.stop_consuming()

                    self._connection.close()
                    break
            except Exception as e:
                # some connection error - reconnect
                self.log.warning("Connection problem - reconnect in 5s : %s", e)
                self.log.debug(e)
                time.sleep(5)
                pass

        # Broke out of the loop
        self.log.info('Exiting cleanly')

    def finish(self):
        """
        Call finish() to close the channel and connection and exit cleanly.
        This invokes the die() callback which can be overridden to clean up.
        """
        if self._channel and self._channel.is_open:
            # Cancel the consumer so that we don't receive more messages
            self._channel.basic_cancel(self._consumer_tag)
        self.stop()
        self._running = False

    def running(self):
        return self._running
