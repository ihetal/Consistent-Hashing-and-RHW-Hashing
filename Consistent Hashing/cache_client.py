import sys
import socket

from sample_data import USERS
from server_config import NODES
from pickle_hash import serialize_GET, serialize_PUT
from node_ring import NodeRing


BUFFER_SIZE = 1024


class UDPClient():

    def send(self, request, server):
        print('Connecting to server at {}:{}'.format(
            server['host'], server['port']))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (server['host'], server['port']))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()

    def replicateData(self, request, replicationServer):
        print('Replicating data to server at {}:{}'.format(
            replicationServer['host'], replicationServer['port']))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(
                request, (replicationServer['host'], replicationServer['port']))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()


def process():
    client_ring = NodeRing()
    hash_codes = set()
    # PUT all users.
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        server, replicationServer = client_ring.get_node_withReplication(key)
        response = UDPClient().send(data_bytes, server)
        print(response)
        UDPClient().replicateData(data_bytes, replicationServer)
        hash_codes.add(str(response.decode()))

    print(
        f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")

    # GET all users.
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_GET(hc)
        server = client_ring.get_node(key)
        response = UDPClient().send(data_bytes, server)
        print(response)


if __name__ == "__main__":

    process()
