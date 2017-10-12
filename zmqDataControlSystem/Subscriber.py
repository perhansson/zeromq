#
#
# Subcriber class
#
#


import zmq
import time
import sys
import ThreadingUtils
from threading import Lock

class Subscriber(ThreadingUtils.MyPyThreading):
    """Base class that subscribes to data."""

    def __init__(self, name='subscriber', context=None, hostname='localhost', socket_nr=5555,  topic='Boss', debug=False):
        """Init."""
        ThreadingUtils.MyPyThreading.__init__(self, name)
        self.name = name
        self.debug = debug
        self.counter = 0
        self.data = None
        self.__flag = False
        self.mutex = Lock()
        c = context
        if c is None:
            c = zmq.Context()
        self.socket = c.socket(zmq.SUB)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)
        self.socket.connect('tcp://%s:%d' % (hostname, socket_nr))


    
    def set_data(self,data):
        """Set data thread safe."""
        self.mutex.acquire()
        try:
            self.data = data
        finally:
            self.mutex.release()


    def reset(self):
        self.__set_flag(False)
        self.set_data(None)
    
            
    def __set_flag(self,f):
        """Set flag data thread safe."""
        self.mutex.acquire()
        try:
            self.__flag = f
        finally:
            self.mutex.release()

    def data_ready(self):
        """Get flag data thread safe."""
        ret = None
        self.mutex.acquire()
        try:
            ret = self.__flag
        finally:
            self.mutex.release()
        return ret
    
    def get_data(self):
        """Get data thread safe."""
        ret = None
        self.mutex.acquire()
        try:
            ret = self.data
            self.counter += 1
        finally:
            self.mutex.release()
        return ret
    
    def run(self):
        """Thread loop."""

        while True:

            # stop if event happened
            if self.stopped():
                break

            try:

                # get message
                msg = self.socket.recv(zmq.DONTWAIT)
                
                topic, data = msg.split()

                # set data
                self.set_data(data)

                # update flag
                self.__set_flag(True)

                if self.debug:
                    print('%s:\t got data: %d' % (self.get_data()))
                
            
                
            except zmq.Again as e:
                pass
                

            
