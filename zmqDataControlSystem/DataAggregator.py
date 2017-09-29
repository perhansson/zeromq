#
# Class that receives messages over PULL socket.
#
#
#

import zmq
import time
import sys
import ThreadingUtils

class ZmqDataAggregator(ThreadingUtils.MyPyThreading):
    """Class that sinks data using zMQ PULL socket."""

    def __init__(self, q):
        """Init."""
        ThreadingUtils.MyPyThreading.__init__(self)
        self.q = q
    
    def run(self):
        """Thread loop."""
        t_start = time.time()
        print('Agg loop start %d' % t_start)
        while not self._stop_event.wait():
            #if self._stop_event.wait():
            #    print('Agg stop event')
            #    break
            data = self.pull()
            self.q.put(data)
            print('Agg loop time %d' % time.time())

            if (time.time() - t_start) % 2 == 0:
                print('Agg loop')
    
    
    def pull(self):
        """Pull message from socket."""
        t_start = time.time()
        # message to receive
        #msg = receiver_socket.recv_string()
        msg = 'msg time %d' % t_start
        print('%d: got message: %s' % (i,msg))
        t_end = time.time()
        #print('Got message in %d seconds' % (t_end-t_start))
        return msg
    


    


