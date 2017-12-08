#
#
#
#
#


import zmq
import time
import sys
import ThreadingUtils
from Subscriber import Subscriber

class DataGenerator(ThreadingUtils.MyPyThreading):
    """Base class that sends data."""

    def __init__(self, name='generator', debug_time=-1):
        """Init."""
        ThreadingUtils.MyPyThreading.__init__(self, name)
        self.name = name
        self.debug_time = debug_time 
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
        return 'processed: %d' % (self.counter)

    def debug_print(self):
        if self.debug_time < 0:
            return False
        elif self.debug_time == 0:
            self.debug = True
        else:
            t = time.time()
            if (t - self.t_last) > self.debug_time:
                self.debug = True
                self.t_last = t
            else:
                self.debug = False




    
class LocalZmqDataGenerator(DataGenerator):
    """Use a locally created message for data to send."""
    
    def __init__(self, name, context=None, socket_host='localhost', socket_nr=5558, debug_time=-1, sleep = -1.0):
        """Init."""
        DataGenerator.__init__(self, name, debug_time)
        self.context = context
        if self.context == None:
            self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.connect('tcp://%s:%d' % (socket_host, socket_nr))
        self.sleep = sleep
    
    def pull(self):
        """Create a message locally."""

        msg = '%s_data%d' % (self.name,self.counter)

        # sleep
        if self.sleep > 0:
            time.sleep(self.sleep)

        return msg

    def send(self, data):
        """Send data."""
        self.socket.send_string(data)
        self.counter += 1
    
        
        

class LocalZmqSubDataGenerator(LocalZmqDataGenerator):
    """Use a locally created message for data to send with flow control controlled by server."""
    
    def __init__(self, name, context=None, socket_sub_nr=5555, socket_sub_host='localhost', socket_host='localhost', socket_nr=5558, debug_time=-1, sleep = -1.0):
        """Init."""
        LocalZmqDataGenerator.__init__(self, name, context, socket_host, socket_nr, debug_time, sleep)
        self.state = 'Stopped'
        self.subscriber = Subscriber(name='%s-subscriber' % name, context=context, socket_nr = socket_sub_nr, hostname=socket_sub_host)

    def stop(self):
        """Stop event."""

        # stop the subsciber
        self.subscriber.stop()
        
        # stop this thread normally through super class method
        super(LocalZmqSubDataGenerator,self).stop()

    
    def start(self):
        """Start thread."""

        # start the subsciber
        self.subscriber.start()
        
        # start this thread normally through super class method
        super(LocalZmqSubDataGenerator,self).start()

    
    def process_sub(self, data):
        """ Process subscriber related tasks."""

        # pull message
        msg = data.decode('utf-8')

        # do stuff with it
        if msg is not None:
            if 'Stopped' in msg:
                self.state = 'Stopped'
            if 'Running' in msg:
                self.state = 'Running'
    
    
    def get_status(self):
        """Return status string."""
        return 'State: %s\tprocessed: %d' % (self.state, self.counter)
    
    
    def run(self):
        """Thread loop."""
        self.t_last = time.time()
        self.debug_print()
        i = 0
        
        while True:
            
            # stop if event happened
            if self.stopped():
                break

            # check if we received a published message
            if self.subscriber.data_ready():

                # get the message
                msg = self.subscriber.get_data()
                # mark data as read
                self.subscriber.reset()

                if self.debug:
                    print('%s:\t data from publisher: \"%s\" ' % (self.name, msg))

                # process message
                self.process_sub(msg)

            
            if self.state is 'Running':
                # get data
                data = self.pull()

                if data is not None:
                    # print stuff
                    #if self.debug:
                    #    print('%s:\tpull_counter %d\tdata %s' % (self.name, i, self.data_str(data)))
                    # send it
                    self.send(data)

            # increase pull counter
            i += 1
            
            # update debug
            self.debug_print()

    
    
        





