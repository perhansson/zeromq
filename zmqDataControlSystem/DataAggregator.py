#
# Class that receives messages over PULL socket.
#
#
#

import zmq
import time
import sys
import ThreadingUtils

class DataAggregator(ThreadingUtils.MyPyThreading):
    """Base class that gets data and puts it into a queue."""

    def __init__(self, q, debug_time=-1):
        """Init."""
        ThreadingUtils.MyPyThreading.__init__(self)
        self.q = q
        self.debug_time = debug_time #print things every X sec
        self.t_last = time.time()
        self.debug = False


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
                break

            # get data
            data = self.pull()

            # print stuff
            if self.debug:
                print('DataAggregator:\tpull_counter %d\tdata %s' % (i, self.data_str(data)))

            # put data into the queue
            self.q.put(data)

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


    def debug_print(self):
        t = time.time()
        if (t - self.t_last) > self.debug_time:
            self.debug = True
            self.t_last = t
        else:
            self.debug = False
    



    
class LocalDataAggregator(DataAggregator):
    """Use a locally created message for data."""
    
    def __init__(self, q, debug_time=-1, sleep = 1.0):
        """Init."""
        DataAggregator.__init__(self, q, debug_time)
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

    def __init__(self, q, debug_time=-1):
        """Init."""
        DataAggregator.__init__(self, q, debug_time)
    
    
    def pull(self):
        """Pull message from socket."""
        #t_last = time.time()
        # message to receive
        msg = receiver_socket.recv_string()
        #t_end = time.time()
        #print('Got message in %d seconds' % (t_end-t_last))
        return msg
    

    


