#
# 
# Data aggregator classes
#
#

import sys
import time
import zmq
import queue
import ThreadingUtils

class DataAggregator(ThreadingUtils.MyPyThreading):
    """Base class that gets data and puts it into a queue."""

    def __init__(self, name='aggegator', q=None, debug_time=-1):
        """Init."""
        ThreadingUtils.MyPyThreading.__init__(self, name)
        self.name = name
        self.q = q
        if self.q == None:
            self.q = queue.Queue()
        self.debug_time = debug_time #print things every X sec
        self.t_last = time.time()
        self.debug = False
        self.counter = 0


    def open(self):
        """Do stuff here before start of thread."""

    def close(self):
        """Do stuff here before thread is killed."""
    
    
    def run(self):
        """Thread loop."""
        self.t_last = time.time()
        self.debug_print()
        i = 0

        while True:

            # stop if event happened
            if self.stopped():
                self.q.put(None)
                break

            # get data
            data = self.pull()

            if data is not None:
                # print stuff
                #if self.debug:
                #    print('%s:\tpull_counter %d\tdata %s' % (self.name, i, self.data_str(data)))

                # put data into the queue
                self.q.put(data)

                # update counter
                self.counter += 1

            # increase counter
            i += 1

            # update debug
            self.debug_print()
    
    
    
    def pull(self):
        """Override this."""
        return ''

    def data_str(self, data):
        """Converts data to human readable string if needed."""
        return data

    def get_status(self):
        """Return status string."""
        return 'processed %d' % self.counter

    def debug_print(self):
        if self.debug_time <= 0:
            return
        t = time.time()
        if (t - self.t_last) > self.debug_time:
            self.debug = True
            self.t_last = t
        else:
            self.debug = False
    



    
class LocalDataAggregator(DataAggregator):
    """Use a locally created message for data."""
    
    def __init__(self, name, q, debug_time=-1, sleep = 1.0):
        """Init."""
        DataAggregator.__init__(self, name, q, debug_time)
        self.i = 0
        self.sleep = sleep
    
    
        
    def pull(self):
        """Create a message locally."""

        msg = '%s_data%d' % (self.name,self.i)
        self.i += 1

        # sleep
        time.sleep(self.sleep)

        return msg
    




class ZmqDataAggregator(DataAggregator):
    """Use zMQ PULL socket for data."""

    def __init__(self, name, q, context, socket_nr=5558, debug_time=-1):
        """Init."""
        DataAggregator.__init__(self, name, q, debug_time)
        self.context = context
        self.socket_nr = socket_nr
        self.socket = self.context.socket(zmq.PULL)
        self.socket.bind('tcp://*:%d' % self.socket_nr)

    def pull(self):
        """Pull message from socket."""
        msg = None
        try:
            msg = self.socket.recv_string(zmq.DONTWAIT)
        except zmq.Again as e:
            # maybe do something here if needed
            pass
        return msg
    



