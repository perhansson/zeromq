#
# Server that subscribes to messages
#
#
#

import zmq
import time

context = zmq.Context()

# subscrice to messages on this socket
sub_socket = context.socket(zmq.SUB)
sub_socket.connect('tcp://localhost:5561')
sub_socket.setsockopt(zmq.SUBSCRIBE, b'')
#134.79.229.18
# subscribe to some messages based on topic
#topicfilt = '101'
#sub_socket.setsockopt_string(zmq.SUBSCRIBE, topicfilt)


# read messages
i = 0
while i<5:

    # receive 
    s = sub_socket.recv()

    print('%i: got message \"%s\"' % (i,s))
    
    i+=1
