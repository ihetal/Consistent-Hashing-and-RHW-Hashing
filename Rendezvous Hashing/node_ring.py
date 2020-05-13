from server_config import NODES
from pickle_hash import hash_code_hex


class NodeRing():

    def __init__(self, nodes=NODES):
        assert len(nodes) > 0
        self.nodes = nodes

    def get_node(self, key_hex):
        max_hash_value = None
        highest_node = None
        for node in self.nodes:
            to_hash = (str(node['port'])+key_hex).encode('utf-8')
            temp_hash = int(hash_code_hex(to_hash), 16)
            if max_hash_value is None or temp_hash > max_hash_value:
                max_hash_value = temp_hash
                highest_node = node

        return highest_node


def test():
    ring = NodeRing(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 node_ring.py
test()
