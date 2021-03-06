#
# Example of zmq client in python
#
#
#


import zmq


context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

for req in range(10):
    print("Sending req %s " % req)
    # bytes
    #socket.send(b"Hello")
    # unicode
    socket.send_string("Hello")

    # bytes
    #server_reply = socket.recv()
    # unicode
    server_reply = socket.recv_string()
    print("Received reply from server for req %d: %s" % (req, server_reply))


