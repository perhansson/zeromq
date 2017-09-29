#
#
#
#
#

import DataAggregator
import DataFuser
import queue
import threading

class DataBoss(object):
    """Data controller."""
    def __init__(self, debug_time=-1):
        self.fuser = None
        self.q = queue.Queue()
        self.aggs = []
        self.debug_time = debug_time
        print('debug_time = %d' % self.debug_time)
    
        
    def start(self):
        """Start the boss."""
        
        # start the fuser
        self.fuser.open()
        self.fuser.start()
        
        
        # start the aggregator threads        
        for agg in self.aggs:
            agg.open()
            agg.start()
        
        print('started threads')
        
    
    def end(self):
        """Stop the boss."""

        # stop queue
        print('stop queue')
        self.q.join()
        
        # stop all threads first
        for agg in self.aggs:
            agg.stop()
            agg.close()
        self.fuser.stop()
        self.fuser.close()

        # now join threads
        for t in self.aggs:
            t.join()
        self.fuser.join()
        print('end threads complete')

    def get_status(self):
        s = 'Queue status:\tqsize %d' % self.q.qsize()
        return s

    def add_agg(self, a):
        self.aggs.append(a)

