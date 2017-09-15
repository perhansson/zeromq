#
# Example of zmq client in python
#
#
#


import zmq


context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://dpm2:5555")

for req in range(10):
    print("Sending req %s " % req)
    socket.send(b"Hello")
    
    server_reply = socket.recv()
    print("Received reply from server for req %d: %s" % (req, server_reply))


