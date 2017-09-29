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
    def __init__(self, q, debug_time=-1):
        """Init."""
        ThreadingUtils.MyPyThreading.__init__(self)
        self.q = q
        self.i = 0
        self.debug_time=debug_time
        self.t_last = time.time()
        self.debug = False
        self.data = None


    def open():
        """ do stuff here if needed"""

    def close():
        """ do stuff here if needed"""
        
    
    def run(self):
        """Thread loop."""
        self.t_last = time.time()
        self.debug_print()
        while True:

            # stop if event happened
            if self.stopped():
                break

            # process data
            self.get_data()

            # process the data
            self.process_data()
    
            # update time
            self.debug_print()
    
    def get_data(self):

        # get data if not empty
        if not self.q.empty():

            if self.debug:
                print('DataFuser: counter %d waiting for data' % self.i)

            # get data from queue
            self.data = self.q.get(True, 1.)         

            # finish queue task
            self.q.task_done()

            # increase counter
            self.i += 1



            
    def process_data(self):
        if self.debug:
            print('DataFuser: counter %d\t process data %s' % (self.i, self.data_str(self.data)))


            
    def data_str(self,data):
        if data is None:
            return 'NULL'
        return '%s' % data

    def debug_print(self):
        t = time.time()
        if (t - self.t_last) > self.debug_time:
            self.debug = True
            self.t_last = t
        else:
            self.debug = False



class SimpleFileDataFuser(DataFuser):
    def __init__(self, q, debug_time=-1, file_name='datafile.dat'):
        """Init."""
        DataFuser.__init__(self, q, debug_time=debug_time)
        self.f = None
        self.file_name = file_name
        print('debug_time %d ' % self.debug_time)

    def open(self):
        self.f = open(self.file_name,'w')

    def close(self):
        self.f.close()

    def process_data(self):
        if self.data is not None:
            self.f.write(self.data + '\n')
            # reset
            self.data = None

