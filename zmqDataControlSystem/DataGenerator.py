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
                print('%s:\tpull_counter NOOO %d\tdata %s' % (self.name, i, self.data_str(data)))

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
    
    def __init__(self, name, socket_host='localhost', socket_nr=5558, debug_time=-1, sleep = -1.0):
        """Init."""
        DataGenerator.__init__(self, name, debug_time)
        self.i = 0
        self.context = None
        self.socket = None
        self.socket_nr = socket_nr
        self.socket_host = socket_host
        self.setup_socket()
        self.sleep = sleep
    
    def setup_socket(self):
        """Setup socket details."""
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.connect('tcp://%s:%d' % (self.socket_host, self.socket_nr))
        
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
    
        
        

class LocalZmqSubDataGenerator(LocalZmqDataGenerator):
    """Use a locally created message for data to send with flow control controlled by server."""
    
    def __init__(self, name, socket_sub_nr=5555, socket_sub_host='localhost', socket_host='localhost', socket_nr=5558, debug_time=-1, sleep = -1.0):
        """Init."""
        LocalZmqDataGenerator.__init__(self, name, debug_time)
        self.state = 'Stopped'
        self.i = 0
        self.socket_sub = None
        self.socket_sub_nr = socket_sub_nr
        self.socket_sub_host = socket_sub_host
        self.setup_sub_socket()
    
    def setup_sub_socket(self):
        """Setup socket details."""
        # setup subscriber socket
        self.socket_sub = self.context.socket(zmq.SUB)
        s = 'tcp://%s:%d' % (self.socket_sub_host, self.socket_sub_nr)
        self.socket_sub.connect(s)
        

    def pull_sub(self):
        """Get messages from publisher.
        Note that this will hang until a message is fully completed.
        """

        msg = None
        #while True:
        try:
            print('DataGenerator:\t pull pub msg')
            s = self.socket_sub.recv()
            #s = self.socket_sub.recv(flags=zmq.NOBLOCK)
            msg = s
        except zmq.Again as e:
            pass
        # use some standardized way to find end of message?
        #if '\f' in s:
        #    break

        return msg


    def process_sub(self):
        """ Process subscriber related tasks."""

        print('DataGenerator:\t go and fetch pub msg')

        # this call should happen in a separate thread
        msg = self.pull_sub()

        print('DataGenerator:\t sub msg \"%s\"' % (msg))

        if msg is not None:
            
            if 'Stopped' in msg:
                self.state = 'Stopped'
            
            if 'Running' in msg:
                self.state = 'Running'
    
    

    def get_status(self):
        """Return status string."""
        return 'State: %s\tprocessed: %d' % (self.state, self.i)
    
    
    def run(self):
        """Thread loop."""
        self.t_last = time.time()
        self.debug_print()
        i = 0

        while True:

            # stop if event happened
            if self.stopped():
                break

            # this should be replaced by a thread that updates stuff in the background.
            self.process_sub()

            # check state
            if self.state == 'Stopped':
                continue

            # get data
            data = self.pull()

            # print stuff
            if self.debug:
                print('%s:\tpull_counter PELLE %d\tdata %s' % (self.name, i, self.data_str(data)))

            # send data
            self.send(data)

            # increase counter
            i += 1

            # update debug
            self.debug_print()
    
    
    
        





