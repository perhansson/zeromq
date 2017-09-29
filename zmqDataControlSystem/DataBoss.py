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
    def __init__(self):
        self.fuser = None
        self.q = queue.Queue()
        self.num_agg_threads = 1
        self.aggs = []
    
        
    def start(self):
        """Start the boss."""
        
        # create fuser thread
        self.fuser = DataFuser.DataFuser(self.q)
        #self.fuser_thread = threading.Thread(target=self.fuser_worker, args=(self.fuser_thread_kill_event, "fuser_task"))
        
        # start the thread
        self.fuser.start()
        
        
        # create the aggregator threads        
        for i in range(self.num_agg_threads):

            # create thread
            agg = DataAggregator.ZmqDataAggregator(self.q)
            
            # start the thread
            agg.start()
            
            self.aggs.append(agg)

        print('started threads')
        
    
    def end(self):
        """Stop the boss."""
        
        for t in self.aggs:
            print('Kill agg thread')
            t.stop()
            t.join()
            print('Kill agg thread')
        
        
        print('Kill fuser thread')
        self.fuser.stop()
        self.fuser.join()
        print('Killed fuser thread')

    def print_status(self):

        print('fuser thread: %d' % self.fuser.is_alive())
        for t in self.aggs:
            print('agg thread: %d' % t.is_alive())
    
        
