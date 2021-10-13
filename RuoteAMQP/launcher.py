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

import json
import logging
import pika

logger = logging.getLogger(__name__)

class Launcher(object):
    """
    A Launcher will launch a Ruote process.

    Workitems arrive via AMQP, are processed and returned to the Ruote engine.

    Cancel is not yet implemented.
    """

    def __init__(self,
                 amqp_host="localhost", amqp_user="boss",
                 amqp_pass="boss", amqp_vhost="boss",
                 conn=None):
        if conn is not None:
            self.conn = conn
        else:
            port = 5672
            if ":" in amqp_host:
                (amqp_host, port) = amqp_host.split(":")
            self.host = amqp_host
            self.user = amqp_user
            self.pw = amqp_pass
            self.vhost = amqp_vhost
            self._conn_params = dict(
                host=amqp_host,
                port=port,
                virtual_host=amqp_vhost,
                heartbeat=5,
                credentials=pika.PlainCredentials(amqp_user, amqp_pass)
            )
            self.conn = pika.BlockingConnection(pika.ConnectionParameters(**self._conn_params))

        if self.conn is None:
            raise Exception(f"Could not connect to "
                            "amqp:{self.host}/{self.vhost}")
        self.chan = self.conn.channel()
        if self.chan is None:
            raise Exception("Could not get a channel on "
                            "amqp:{self.host}/{self.vhost}")

    def launch(self, process, fields=None, variables=None):
        """
        Launch a process definition
        """
        if fields and not isinstance(fields, dict):
            raise TypeError("fields should be type dict")
        if variables and not isinstance(variables, dict):
            raise TypeError("variables should be type dict")
        pdef = {
            "definition": process,
            "fields": fields,
            "variables": variables
            }
        # Encode the message as json
        msg = json.dumps(pdef)
        logger.debug(f"Launching {pdef}")
        # delivery_mode=2 is persistent
        # Publish the message.
        self.chan.basic_publish('',  # Exchange
                                'ruote_workitems',  # Routing key
                                msg,
                                pika.BasicProperties(content_type='text/plain',
                                                     delivery_mode=2))

    def resume(self, workitem : "Workitem" = None, msg=None):
        """
        Resume a stored workitem
        """
        if workitem:
            msg = workitem.dump()
        if msg is None:
            raise Exception("Either workitem or msg must be provided")
        # delivery_mode=2 is persistent
        # Publish the message.
        self.chan.basic_publish('',  # Exchange
                                'ruote_workitems',  # Routing key
                                body=msg,
                                properties=pika.BasicProperties(
                                    content_type='text/plain',
                                    delivery_mode=2))
