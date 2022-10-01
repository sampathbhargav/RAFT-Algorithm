from flask import Flask, render_template, request, redirect, url_for, jsonify
from flask_cors import CORS

import json
import socket
import traceback
import time
import sys

app = Flask(__name__)

cors = CORS(app)

sender = "client"

# Socket Creation 
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)


# open index.html at client side on running
@app.route('/')
def clientHome():
    return render_template("index.html")

# @GetMapping("/sendMessage")
@app.route('/sendMessage', methods = ['POST'])
def sendMsg():
    print("Starting Controller to test STORE")

    # Read Message Template
    msg = json.load(open("Message.json"))

    # data = request.get_json() 
    resp = {}
    if request.method == "POST":
        #seed(1)
        msgg = request.form['message_input']
        nd = request.form.get('Nodes')

        print("message is : " + msgg)
        print("Node is : " + str(nd))

        # msgg = data['message']
        # nd = data['node']

        target = str(nd)

        # Request
        msg['sender_name'] = sender
        msg['request'] = "STORE"
        msg['key'] = "x"
        msg['value'] = msgg

        app.logger.info("Request Created : {}".format(msg))
        print(f"Request Created : {msg}")

        try:
            skt.sendto(json.dumps(msg).encode('utf-8'), (target, 5555))
            skt.settimeout(None)
            app.logger.info("Sent request to : {}".format(target))
            print("Sent request to "+target)
        except socket.timeout:
            print("caught a timeout")
        except:
            #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
            print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

        try:
            skt.settimeout(4.0)
            resp, addr = skt.recvfrom(1024)
            skt.settimeout(None)
            resp = json.loads(resp.decode('utf-8'))
            app.logger.info("resp is : {}".format(resp))
            print("Response received ", resp)
            # socketio.emit('receive_message', resp)
        except socket.timeout:
            app.logger.info("caught a timeout")
            print("timedout")
        except:
            print("Error", traceback.format_exc())

        print("Done")

        return render_template("index.html", msg=resp)
    else:
        return render_template("index.html")

# @app.route('/receiveMessage')
# def receiveMessage():

#     try:
#         skt.settimeout(2.0)
#         resp, addr = skt.recvfrom(1024)
#         skt.settimeout(None)
#         resp = json.loads(resp.decode('utf-8'))
#         print("Response received ", resp)
#         # socketio.emit('receive_message', resp)
#     except socket.timeout:
#         print("caught a timeout")
#     except:
#         print("Error", traceback.format_exc())

#     print("Done")


if __name__ == '__main__':
    # Initialize
    port = 5555
    skt.bind((sender, port))
    app.run(debug=True, host='0.0.0.0')