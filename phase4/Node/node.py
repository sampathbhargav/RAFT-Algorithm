import socket
import time
from kthread import *
import json
import traceback
import os
import random
import pathlib


class Node:
    def __init__(self, id):
        print("Node ", id, " started")
        self.lastUpdated = 0
        self.currentLeader = None
        self.id = id
        self.name = "Node" + str(self.id)
        self.currentTerm = 0
        self.state = 'follower'
        self.votedFor = None
        self.commitIndex = 0
        self.sentLength = [0]*5
        self.ackedLength = [0]*5
        self.log = []
        self.port = 5555
        self.electionInterval = random.randint(150, 300) * 0.001
        self.heartbeatInterval = self.electionInterval/3.0
        self.votes = 0
        self.peers = list(range(1, 6))
        self.peers.remove(self.id)
        udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        udp_socket.bind((self.name, self.port))
        self.socket = udp_socket
        self.election = None
        self.leader_state = None
        self.loadStateInfo()
        self.follower_state = KThread(target=self.follower)
        self.follower_state.start()
        self.listener = KThread(target=self.listener)
        self.listener.start()

    def follower(self):
        print("Running as a follower ", self.id)
        self.state = 'follower'
        self.votes = 0
        self.votedFor = None
        self.currentLeader = None
        self.lastUpdated = time.time()
        self.persist()
        while time.time() - self.lastUpdated < self.electionInterval:
            pass
        if self.election and self.election.is_alive():
            self.election.kill()
        self.election = KThread(target=self.startElection)
        self.election.start()

    def startElection(self):
        print("Becoming candidate and starting election ", self.id)
        self.state = 'candidate'
        self.currentTerm += 1
        self.votedFor = self.name
        self.votes += 1
        self.persist()

        lastTerm = 0
        if len(self.log) > 0:
            lastTerm = self.log[-1]["Term"]

        msg = json.load(open("Message.json"))
        msg["request"] = "RequestVoteRPC"
        msg["term"] = self.currentTerm
        msg["sender_name"] = self.name
        msg["prevLogLength"] = len(self.log)
        msg["prevLogTerm"] = lastTerm
        msg["sender_id"] = self.id

        for targetId in self.peers:
            target = "Node" + str(targetId)
            try:
                self.socket.sendto(json.dumps(msg).encode('utf-8'), (target, self.port))
            except:
                pass

    def leader(self):
        print("Became leader ", self.id)
        self.state = 'leader'
        self.persist()
        while True:
            for targetId in self.peers:
                self.replicateLog(targetId)
            time.sleep(self.heartbeatInterval)

    def replicateLog(self, followerId):
        prefixLen = self.sentLength[followerId - 1]
        entry = self.log[prefixLen:]
        prefixTerm = 0
        if prefixLen > 0:
            prefixTerm = self.log[prefixLen-1]["Term"]

        msg = json.load(open("Message.json"))
        msg["request"] = "AppendEntryRPC"
        msg["term"] = self.currentTerm
        msg["sender_name"] = self.name
        msg["sender_id"] = self.id
        msg["prevLogTerm"] = prefixTerm
        msg["prefixLogLen"] = prefixLen
        msg["commitIndex"] = self.commitIndex
        msg["entry"] = entry

        target = "Node" + str(followerId)
        try:
            self.socket.sendto(json.dumps(msg).encode('utf-8'), (target, self.port))
        except:
            pass

    def appendEntries(self, prefixLen, leaderCommit, entry):
        if len(entry) > 0 and len(self.log) > prefixLen:
            index = min(len(self.log), prefixLen + len(entry)) - 1
            if self.log[index]["Term"] != entry[index - prefixLen]["Term"]:
                self.log = self.log[:prefixLen]

        if prefixLen + len(entry) > len(self.log):
            for i in range(len(self.log) - prefixLen, len(entry)):
                self.log.append(entry[i])

        if leaderCommit > self.commitIndex:
            self.commitIndex = leaderCommit

        self.persist()

    def commitLogEntries(self):
        ready = []
        for i in range(1, len(self.log) + 1):
            minEntries = 0
            for node in range(5):
                if self.ackedLength[node] >= i:
                    minEntries += 1
            if minEntries >= 3:
                ready.append(i)

        if len(ready) != 0 and max(ready) > self.commitIndex and self.log[max(ready) - 1]["Term"] == self.currentTerm:
            self.commitIndex = max(ready)

    def becomeFollower(self):
        print("stepped down to follower")
        if self.election and self.election.is_alive():
            self.election.kill()
        if self.leader_state and self.leader_state.is_alive():
            self.leader_state.kill()
        if self.follower_state and self.follower_state.is_alive():
            self.follower_state.kill()
        self.persist()
        self.follower_state = KThread(target=self.follower)
        self.follower_state.start()

    def listener(self):
        while True:
            try:
                resMsg, addr = self.socket.recvfrom(1024)
            except:
                print(f"ERROR while fetching from socket : {traceback.print_exc()}")
            msg = json.loads(resMsg.decode('utf-8'))
            if msg["request"] == "STORE":
                print("STORE req ", msg)
                if self.state == 'leader':
                    self.log.append({"Term": self.currentTerm,
                                     "Key": msg["key"],
                                     "Value": msg["value"]})
                    self.ackedLength[self.id - 1] = len(self.log)
                    self.persist()
                    for targetId in self.peers:
                        self.replicateLog(targetId)
                else:
                    response = {"sender_name": self.name,
                                "term": self.currentTerm,
                                "request": "LEADER_INFO",
                                "key": "LEADER",
                                "value": "Node" + str(self.currentLeader)}
                    self.socket.sendto(json.dumps(response).encode('utf-8'), ("Controller", self.port))
                    self.socket.sendto(json.dumps(response).encode('utf-8'), ("client", 4000))
                    print("sent to client\n")

            elif msg["request"] == 'RETRIEVE':
                print("RETRIEVE req ", msg)
                if self.state == 'leader':
                    response = {"sender_name": self.name,
                                "term": self.currentTerm,
                                "request": "RETRIEVE",
                                "key": "COMMITED_LOGS",
                                "value": self.log}
                else:
                    response = {"sender_name": self.name,
                                "term": self.currentTerm,
                                "request": "LEADER_INFO",
                                "key": "LEADER",
                                "value": "Node" + str(self.currentLeader)}
                self.socket.sendto(json.dumps(response).encode('utf-8'), ("Controller", self.port))
                #self.socket.sendto(json.dumps(response).encode('utf-8'), ("client", self.port))

            elif msg["request"] == "CONVERT_FOLLOWER":
                if self.state != 'follower':
                    if self.election and self.election.is_alive():
                        self.election.kill()
                    if self.leader_state and self.leader_state.is_alive():
                        self.leader_state.kill()
                    time.sleep(0.5)
                    self.follower_state = KThread(target=self.follower)
                    self.follower_state.start()

            elif msg["request"] == "TIMEOUT":
                self.lastUpdated = self.lastUpdated - self.electionInterval

            elif msg["request"] == "SHUTDOWN":
                if self.follower_state and self.follower_state.is_alive():
                    self.follower_state.kill()
                if self.election and self.election.is_alive():
                    self.election.kill()
                if self.leader_state and self.leader_state.is_alive():
                    self.leader_state.kill()
                if self.listener and self.listener.is_alive():
                    self.listener.kill()

            elif msg["request"] == "LEADER_INFO":
                if self.state == 'leader':
                    response = {"sender_name": self.name,
                                "term": self.currentTerm,
                                "key": "LEADER",
                                "value": self.state}
                else:
                    response = {"sender_name": self.name,
                                "term": self.currentTerm,
                                "key": "LEADER",
                                "value": "Node" + str(self.currentLeader)}
                self.socket.sendto(json.dumps(response).encode('utf-8'), ("Controller", self.port))

            elif msg["request"] == "AppendEntryReplyRPC":
                print("AppendEntryReplyRPC req ", msg)
                if msg["term"] == self.currentTerm and self.state == 'leader':
                    if msg["success"] == "true" and msg["ack"] >= self.ackedLength[msg["sender_id"] - 1]:
                        self.sentLength[msg["sender_id"] - 1] = msg["ack"]
                        self.ackedLength[msg["sender_id"] - 1] = msg["ack"]
                        self.commitLogEntries()
                    elif self.sentLength[msg["sender_id"] - 1] > 0:
                        self.sentLength[msg["sender_id"] - 1] -= 1
                        self.replicateLog(msg["sender_id"])
                elif msg["term"] > self.currentTerm:
                    self.currentTerm = msg["term"]
                    if self.state != 'follower':
                        self.becomeFollower()

            elif msg["request"] == "AppendEntryRPC":
                print("AppendEntryRPC req ", msg)
                if msg["term"] >= self.currentTerm:
                    self.currentTerm = msg["term"]
                    self.currentLeader = msg["sender_id"]
                    self.lastUpdated = time.time()
                    self.persist()
                    if self.state != 'follower':
                        self.becomeFollower()
                logOk = len(self.log) >= msg["prefixLogLen"] and (
                            msg["prefixLogLen"] == 0 or self.log[msg["prefixLogLen"] - 1]["Term"] == msg["prevLogTerm"])
                resp_msg = json.load(open("Message.json"))
                resp_msg["request"] = "AppendEntryReplyRPC"
                resp_msg["sender_name"] = self.name
                resp_msg["term"] = self.currentTerm
                resp_msg["sender_id"] = self.id
                if msg["term"] == self.currentTerm and logOk:
                    self.appendEntries(msg["prefixLogLen"], msg["commitIndex"], msg["entry"])
                    resp_msg["ack"] = msg["prefixLogLen"] + len(msg["entry"])
                    resp_msg["success"] = "true"
                else:
                    resp_msg["ack"] = 0
                    resp_msg["success"] = "false"
                self.socket.sendto(json.dumps(resp_msg).encode('utf-8'), (msg["sender_name"], self.port))

            elif msg["request"] == "RequestVoteRPC":
                print("RequestVoteRPC req", msg)
                candidateName = "Node" + str(msg["sender_id"])
                if msg["term"] > self.currentTerm:
                    self.currentTerm = msg["term"]
                    if self.state != 'follower':
                        self.becomeFollower()
                    self.votedFor = None
                lastTerm = 0
                if len(self.log) > 0:
                    lastTerm = self.log[-1]["Term"]
                logOk = msg["prevLogTerm"] > lastTerm or (
                            msg["prevLogTerm"] == lastTerm and msg["prevLogLength"] >= len(self.log))
                if msg["term"] == self.currentTerm and logOk and self.votedFor in [msg["sender_name"], None]:
                    voteGranted = True
                    self.votedFor = msg["sender_name"]
                else:
                    voteGranted = False
                self.persist()
                try:
                    resp_msg = json.load(open("Message.json"))
                    resp_msg["request"] = "RequestVoteReplyRPC"
                    resp_msg["term"] = self.currentTerm
                    resp_msg["sender_name"] = self.name
                    resp_msg["sender_id"] = self.id
                    resp_msg["voteGranted"] = "true" if voteGranted else "false"
                    self.socket.sendto(json.dumps(resp_msg).encode('utf-8'), (candidateName, self.port))
                except:
                    pass

            elif msg["request"] == "RequestVoteReplyRPC":
                self.lastUpdated = time.time()
                print("RequestVoteReplyRPC request on ", self.id, " ", msg)
                if self.state == 'candidate' and self.currentTerm == msg["term"] and msg["voteGranted"] == "true":
                    self.votes += 1
                    if self.votes == 3:
                        if self.election.is_alive():
                            self.election.kill()
                        if self.follower_state.is_alive():
                            self.follower_state.kill()
                        self.leader_state = KThread(target=self.leader)
                        self.leader_state.start()
                        for peer in self.peers:
                            self.sentLength[peer - 1] = len(self.log)
                            self.ackedLength[peer - 1] = 0
                            self.replicateLog(peer)

                elif int(msg["term"]) > self.currentTerm:
                    self.currentTerm = msg["term"]
                    if self.state != 'follower':
                        self.becomeFollower()
                        self.persist()

    def persist(self):
        msg = {
               "currentTerm": self.currentTerm,
               "votedFor": self.votedFor,
               "Log[]": self.log,
               "Timeout": self.electionInterval,
               "Heartbeat": self.heartbeatInterval,
               "commitIndex": self.commitIndex
        }
        with open("/data/config" + str(self.id) + ".json", 'w') as f:
            json.dump(msg, f)

    def loadStateInfo(self):
        pathlib.Path('/data').mkdir(exist_ok=True)
        file = "/data/config" + str(self.id) + ".json"
        if os.path.exists(file):
            msg = json.load(open(file))
            self.currentTerm = msg["currentTerm"]
            self.votedFor = msg["votedFor"]
            self.log = msg["Log[]"]
            self.electionInterval = msg["Timeout"]
            self.heartbeatInterval = msg["Heartbeat"]
            self.commitIndex = msg["commitIndex"]

if __name__ == "__main__":
    time.sleep(0.1)
    id = int(os.environ["id"])
    node = Node(id)