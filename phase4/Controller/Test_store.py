import json
import socket
import traceback
import time
import sys

if __name__ == "__main__":

    print("Starting Controller to test STORE")

    # Read Message Template
    msg = json.load(open("Message.json"))

    # Initialize
    sender = "Controller"
    target = sys.argv[1]
    port = 5555

    # Request
    msg['sender_name'] = sender
    msg['request'] = "STORE"
    msg['key'] = "x"
    msg['value'] = "1"

    print(f"Request Created : {msg}")

    # Socket Creation and Binding
    skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skt.bind((sender, port))

    try:
        skt.sendto(json.dumps(msg).encode('utf-8'), (target, 5555))
        skt.settimeout(None)
        print("Sent request to "+target)
    except socket.timeout:
        print("timedout")
    except:
        #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

    try:
        skt.settimeout(2.0)
        resp, addr = skt.recvfrom(1024)
        skt.settimeout(None)
        resp = json.loads(resp.decode('utf-8'))
        print("Response received ", resp)
    except socket.timeout:
        print("timedout")
    except:
        print("Error", traceback.format_exc())

    print("Done")