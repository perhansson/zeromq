#
#
#
#
#

import threading

class MyPyThreading(threading.Thread):

    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name
        self._stop_event = threading.Event()


    def run(self):
        i = 0
        while i<3:
            print('MyPyThreading %s run %d' % (self.name, i))
            i += 1
    

    def stop(self):
        self._stop_event.set()
        print('stopped thread %s (stop flag: %d)' % (self.name, self.stopped()))
    

    def stopped(self):
        return self._stop_event.is_set()
    
