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
        self.aggs = [] # data aggregators
        self.gens = [] # generators
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


        # start the generator threads        
        for gen in self.gens:
            gen.open()
            gen.start()

        print('started threads')
        
    
    def end(self):
        """Stop the boss."""

        # stop the data generating threads
        for gen in self.gens:
            gen.stop()
            gen.close()
            gen.join()

        # stop the queue and wait for each to be processed
        self.q.join()

        # stop adding to the queue so that it can fully process
        for agg in self.aggs:
            agg.stop()
            agg.close()

        # stop aggregators now
        for agg in self.aggs:
            agg.join()

        # stop the fuser
        self.fuser.stop()
        self.fuser.close()
        self.fuser.join()
        
        print('end threads complete')

    
    def get_status(self):
        """Get status."""
        s = 'Queue status:\tqsize %d' % self.q.qsize()
        for a in self.aggs:
            s += '\n%s status: %s' % (a.name,a.get_status())
        for g in self.gens:
            s += '\n%s status: %s' % (g.name,g.get_status())
        return s


    def add_agg(self, a):
        """ Add data aggregator."""
        self.aggs.append(a)


    def add_gen(self, g):
        """ Add data generator."""
        self.gens.append(g)

