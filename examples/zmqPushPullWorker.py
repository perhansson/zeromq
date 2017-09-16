#
# Worker that sends messages.
# Sends messages to PUSH socket
#
#
#

import zmq
import time
import sys
import argparse

def get_args():
    parser = argparse.ArgumentParser('Push-Pull Worker')
    parser.add_argument('--name', default='Pelle', help='Name of worker')
    parser.add_argument('--rate', type=float, default=1.0, help='Frequency of message in Hz.')
    a = parser.parse_args()
    return a


def generate_message():
    t = time.time()
    msg = 'time_%d' % (t)
    return msg 


if __name__ == '__main__':

    args = get_args()
    
    # counter
    i=0
    
    context = zmq.Context()
    
    # send messages to this socket
    sender_socket = context.socket(zmq.PUSH)
    sender_socket.connect('tcp://localhost:5558')
    
    # process events
    while True:

        # generate message to send
        msg = 'Worker \"%s\": %s ' % (args.name, generate_message())

        # print to screen
        print(msg)

        # print progress
        sys.stdout.write('.')
        sys.stdout.flush()

        # sleep a little
        time.sleep(1.0/args.rate)

        # send the message
        sender_socket.send_string(msg)

        # count
        i += 1

