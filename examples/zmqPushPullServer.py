#
# Server that receives messages.
# Binds to PULL socket.
#
#
#

import zmq
import time
import sys

i=0

context = zmq.Context()

# receive messages to this socket
receiver_socket = context.socket(zmq.PULL)
receiver_socket.bind('tcp://*:5558')

# Start timer
t_start = time.time()

# process n events
while True:

    # message to send
    msg = receiver_socket.recv_string()

    print('%d: got message: %s' % (i,msg))
    
    i += 1

t_end = time.time()

print('Got all messages in %d sec' % (t_end-t_start))


