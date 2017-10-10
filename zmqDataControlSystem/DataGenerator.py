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
        t = time.time()
        if (t - self.t_last) > self.debug_time:
            self.debug = True
            self.t_last = t
        else:
            self.debug = False
    



    
class LocalZmqDataGenerator(DataGenerator):
    """Use a locally created message for data to send."""
    
    def __init__(self, name, context, socket_host='localhost', socket_nr=5558, debug_time=-1, sleep = -1.0):
        """Init."""
        DataGenerator.__init__(self, name, debug_time)
        self.context = context
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
    
    def __init__(self, name, context, socket_sub_nr=5555, socket_sub_host='localhost', socket_host='localhost', socket_nr=5558, debug_time=-1, sleep = -1.0):
        """Init."""
        LocalZmqDataGenerator.__init__(self, name, context, socket_host, socket_nr, debug_time, sleep)
        self.state = 'Running'
        self.socket_sub = self.context.socket(zmq.SUB)
        self.socket_sub.setsockopt_string(zmq.SUBSCRIBE, 'Boss')
        self.socket_sub.connect('tcp://%s:%d' % (socket_sub_host, socket_sub_nr))
    

    def pull_sub(self):
        """Get messages from publisher.
        Note that this will hang until a message is fully completed.
        """
        msg = None
        try:
            s = self.socket_sub.recv(zmq.DONTWAIT)
            topic, messagedata = s.split()
            # it needs to be decoded, even though it was publihsed as string?
            msg = messagedata.decode('utf-8') 
        except zmq.Again as e:
            pass
        return msg
    

    def process_sub(self):
        """ Process subscriber related tasks."""

        # pull message
        msg = self.pull_sub()

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

            # this should be replaced by a thread that updates stuff in the background.
            self.process_sub()


            # send data
            if self.state is 'Running':
                # get data
                data = self.pull()

                if data is not None:
                    # print stuff
                    #if self.debug:
                    #    print('%s:\tpull_counter %d\tdata %s' % (self.name, i, self.data_str(data)))
                    # send it
                    self.send('data')

            # increase pull counter
            i += 1
            
            # update debug
            self.debug_print()

    
    
        





