#
# Class that fuses data together.
#
#
#

import time
import sys
import ThreadingUtils

class DataFuser(ThreadingUtils.MyPyThreading):
    """Class that receives data and merges it."""
    def __init__(self, q):
        """Init."""
        ThreadingUtils.MyPyThreading.__init__(self)
        self.q = q
    
    def run(self):
        """Thread loop."""
        t_start = time.time()
        print('DataFuser loop start %d' % t_start)
        print('DataFuser stop: %d' % self._stop_event.is_set())
        while True:
            print('DataFuser loop time %d' % time.time())
            print('DataFuser stop: %d' % self._stop_event.is_set())
            if self._stop_event.wait():
            #    #if self._stop_event:
            #    print('DataFuser stop event')
            #    print('DataFuser stop: %d' % self._stop_event.is_set())
            sys.stdout.flush()
            #    break
            
            #data = self.q.get()
            #
            #if data is not None:
            #    self.process(data)
            #
            #self.q.task_done()
            #
            #if (t_start - time.time()) % 2 == 0:
            #    print('DataFuser loop')
        print('DataFuser loop time %d' % time.time())

    
    def process(self,data):
        self.print_data(data)

    def print_data(self,data):
        print("Processing data: %s" % (data))

