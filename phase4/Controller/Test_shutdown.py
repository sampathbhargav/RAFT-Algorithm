import json
import socket
import traceback
import time
import sys


if __name__ == "__main__":

    print("Starting Controller to test SHUTDOWN")

    # Read Message Template
    msg = json.load(open("Message.json"))

    # Initialize
    sender = "Controller"
    target = sys.argv[1]
    port = 5555

    # Request
    msg['sender_name'] = sender
    msg['request'] = "SHUTDOWN"
    print(f"Request Created : {msg}")

    # Socket Creation and Binding
    skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skt.bind((sender, port))


    try:
        skt.sendto(json.dumps(msg).encode('utf-8'), (target, 5555))
        skt.settimeout(None)
        print("Sent request to "+target)
    except socket.timeout:
        print("caught a timeout")
    except:
        #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


    print("Done")