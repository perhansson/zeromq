#
#
# Publisher class
#
#


import zmq
import time
import sys
import ThreadingUtils
from threading import Lock

class Publisher():
    """Base class that publishes data."""

    def __init__(self, name='publisher', context=None, hostname='localhost', socket_nr=5555,  topic='Boss', debug=False):
        """Init."""
        self.name = name
        self.debug = debug
        self.counter = 0
        self.topic = topic
        self.socket_nr = socket_nr
        c = context
        if c is None:
            c = zmq.Context()
        self.socket = c.socket(zmq.PUB)
        self.socket.bind('tcp://*:%d' % self.socket_nr)
    
    def publish(self, data):
        """Publish data."""
        str = self.topic + ' ' + data
        print('%s:\t publish %s' % (self.name, str))
        self.socket.send_string(str)
    

