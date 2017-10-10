#
# Server that publishes messages
# Binds to pub socket
#
#
#

import zmq
import time


context = zmq.Context()

# publish messages on this socket
pub_socket = context.socket(zmq.PUB)
pub_socket.bind('tcp://*:5561')
#134.79.229.18

# publish some messages
topics = range(98,103)
i = 0
while True:

    for topic in topics:
        
        # message to publish
        msg = "%d %s%d" % (topic, "msg nr", i)
        
        # print messagzme that we are sending
        print(msg)
        
        
        # send the message
        pub_socket.send_string(msg)
        #pub_socket.send(b'hej')
        
        # sleep a little
        time.sleep(1)
        
    i+=1
