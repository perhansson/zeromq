#
#
#
#
#


import zmq
import time
import sys
import ThreadingUtils

class DataGenerator(ThreadingUtils.MyPyThreading):
    """Base class that sends data."""

    def __init__(self, name, debug_time=-1):
        """Init."""
        ThreadingUtils.MyPyThreading.__init__(self)
        self.name = name
        self.debug_time = debug_time 
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
                print('%s:\tpull_counter %d\tdata %s' % (self.name, i, self.data_str(data)))

            # send data
            self.send(data)

            # increase counter
            i += 1

            # update debug
            self.debug_print()
    
    
    
    def pull(self):
        """Override this."""
        return ''

    def send(self):
        """Override this."""
        return
    
    def data_str(self, data):
        """Converts data to human readable string if needed."""
        return data

    def get_status(self):
        """Return status string."""
        return 'status'

    def debug_print(self):
        t = time.time()
        if (t - self.t_last) > self.debug_time:
            self.debug = True
            self.t_last = t
        else:
            self.debug = False
    



    
class LocalZmqDataGenerator(DataGenerator):
    """Use a locally created message for data to send."""
    
    def __init__(self, name, socket_nr=5558, debug_time=-1, sleep = -1.0):
        """Init."""
        DataGenerator.__init__(self, name, debug_time)
        self.i = 0
        self.context = None
        self.socket = None
        self.socket_nr = socket_nr
        self.setup_socket()
        self.sleep = sleep
    
    def setup_socket(self):
        """Setup socket details."""
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.connect('tcp://localhost:%d' % self.socket_nr)
        
    def pull(self):
        """Create a message locally."""

        msg = '%s_data%d' % (self.name,self.i)
        self.i += 1

        # sleep
        if self.sleep > 0:
            time.sleep(self.sleep)

        return msg

    def send(self, data):
        """Send data."""
        self.socket.send_string(data)
    
        
        





