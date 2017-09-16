#
# Example of zmq server in python
#
#
#


import time
import zmq


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

while True:

    # bytes
    #message = socket.recv()
    # unicode
    message = socket.recv_string()
    print("Received request: %s" % message)
    
    time.sleep(1)

    # bytes
    #socket.send(b"whatever you say")
    # unicode
    socket.send_string("whatever you say")

