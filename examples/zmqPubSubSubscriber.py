#
# Server that subscribes to messages
#
#
#

import zmq
import time

context zmq.Context()

# subscrice to messages on this socket
sub_socket = context.socket(zmq.PUB)
sub_socket.connect('tcp://localhost:5558')

# subscribe to some messages based on topic
topicfilt = 101
sub_socket.setsockopt(zmq.SUBSCRIBE, topicfilt)


# read messages
i = 0
while i<5:

    # receive 
    s = sub_socket.recv()

    print('%i: got message \"%s\"' % (s))
    
    i+=1
