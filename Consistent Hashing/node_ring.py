from server_config import NODES
from pickle_hash import hash_code_hex
import json
import bisect
import mmh3


class NodeRing():

    def __init__(self, nodes=NODES, hashsize=360, virtuallayers=3):
        assert len(nodes) > 0
        self.nodes = nodes
        self.hashsize = hashsize-1
        self.ring = {}
        self._sorted_keys = []
        self.virtuallayers = virtuallayers
        self._generate_ring()

    def _generate_ring(self):

        for node in self.nodes:
            to_hash = (str(node['port'])).encode('utf-8')
            for i in range(0, self.virtuallayers):
                key = mmh3.hash(to_hash, i) % self.hashsize
                while(key in self.ring):
                    key += 1
                self._sorted_keys.append(key)
                self.ring[key] = node
        self._sorted_keys.sort()

    def get_node(self, key_hex):
        to_hash = key_hex.encode('utf-8')
        key = int(hash_code_hex(to_hash), 16) % self.hashsize
        pos = bisect.bisect(self._sorted_keys, key)
        if(pos >= len(self._sorted_keys)-1):
            pos = 0
        nodeKey = self._sorted_keys[pos]
        return self.ring[nodeKey]

    def get_node_withReplication(self, key_hex):
        to_hash = key_hex.encode('utf-8')
        key = int(hash_code_hex(to_hash), 16) % self.hashsize
        pos = bisect.bisect(self._sorted_keys, key)
        if(pos >= len(self._sorted_keys)-1):
            pos = 0
        nodeKey1 = self._sorted_keys[pos]
        nodeKey2 = self._sorted_keys[pos+1]
        return self.ring[nodeKey1], self.ring[nodeKey2]


def test():
    ring = NodeRing(nodes=NODES, hashsize=3600)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 node_ring.py
# test()
