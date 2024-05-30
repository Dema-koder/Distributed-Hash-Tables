import grpc
import sys
import zlib
from concurrent import futures
import chord_pb2_grpc as pb2_grpc
import chord_pb2 as pb2

node_id = sys.argv[1]

CHORD = [2, 16, 24, 25, 26, 31]
CHANNELS = [
    "127.0.0.1:5000",
    "127.0.0.1:5001",
    "127.0.0.1:5002",
    "127.0.0.1:5003",
    "127.0.0.1:5004",
    "127.0.0.1:5005",
]

data = {}
finger_table = []

M = 5
id = -1
succ = -1
pred = -1


def populate_finger_table(id):

    global finger_table
    global succ
    global pred

    for i in range(1, M + 1):
        cur = (id + 2 ** (i - 1)) % 2**M
        idx = -1
        for chord in CHORD:
            if cur <= chord:
                idx = chord
                break
        if idx == -1:
            idx = CHORD[0]
        finger_table.append(idx)

    def find_successor(target):
        return finger_table[0]

    succ = find_successor(id)

    def find_predecessor(target):
        idx = -1
        for i in range(len(CHORD) - 1, -1, -1):
            if CHORD[i] < target:
                idx = i
                break
        return CHORD[idx]

    pred = find_predecessor(id)

    return


def get_stub(channel):
    channel = grpc.insecure_channel(channel)
    return pb2_grpc.ChordStub(channel)


def get_target_id(key):
    hash_value = zlib.adler32(key.encode())
    return hash_value % (2**M)


def save(key, text):
    global data

    b = not (key in data)
    if b:
        data[key] = text
    return b


def remove(key):
    global data

    b = key in data
    if b:
        del data[key]
    return b


def find(key):
    global data

    b = key in data
    if not b:
        return ""
    return data[key]


class NodeHandler(pb2_grpc.ChordServicer):
    def SaveData(self, request, context):
        key = request.key
        text = request.text
        hash = get_target_id(key)
        status = False

        if pred < hash <= id or (hash > pred > id) or (pred > id >= hash):
            id_node = id
            if save(key, text):
                print(f"Node {id} says: Saved {key}")
                status = True
            else:
                print(f"Node {id} says: Not saved {key}")
        elif id < hash <= succ or (hash > id > succ) or (id > succ >= hash):
            print(f"Node {id} says: Save from {id} to {succ}")
            id_node = succ
            stub = get_stub(f"127.0.0.1:500{CHORD.index(succ)}")
            save_data_response = stub.SaveData(request)
            status = save_data_response.status
        else:
            cur = -1
            for i in range(len(finger_table) - 1):
                if finger_table[i] <= hash:
                    cur = max(cur, finger_table[i])
            if cur == -1:
                cur = max(finger_table)
            print(f"Node {id} says: Save from {id} to {cur}")
            stub = get_stub(f"127.0.0.1:500{CHORD.index(cur)}")
            save_data_response = stub.SaveData(request)
            status = save_data_response.status
            id_node = save_data_response.node_id

        reply = {'status': status, 'node_id': id_node}
        return pb2.SaveDataResponse(**reply)

    def RemoveData(self, request, context):
        key = request.key
        hash = get_target_id(key)
        status = False

        if pred < hash <= id or (hash > pred > id) or (pred > id >= hash):
            id_node = id
            if remove(key):
                print(f"Node {id} says: Removed {key}")
                status = True
            else:
                print(f"Node {id} says: Not removed {key}")
        elif id < hash <= succ or (hash > id > succ) or (id > succ >= hash):
            print(f"Node {id} says: Remove from {id} to {succ}")
            id_node = succ
            stub = get_stub(f"127.0.0.1:500{CHORD.index(succ)}")
            remove_data_response = stub.RemoveData(request)
            status = remove_data_response.status
        else:
            cur = -1
            for i in range(len(finger_table) - 1):
                if finger_table[i] <= hash:
                    cur = max(cur, finger_table[i])
            if cur == -1:
                cur = max(finger_table)
            print(f"Node {id} says: Remove from {id} to {cur}")
            stub = get_stub(f"127.0.0.1:500{CHORD.index(cur)}")
            remove_data_response = stub.RemoveData(request)
            status = remove_data_response.status
            id_node = remove_data_response.node_id

        reply = {'status': status, 'node_id': id_node}
        return pb2.RemoveDataResponse(**reply)

    def FindData(self, request, context):
        key = request.key
        hash = get_target_id(key)
        status = ""

        if pred < hash <= id or (hash > pred > id) or (pred > id >= hash):
            id_node = id
            if find(key) != "":
                print(f"Node {id} says: Found {key}")
                status = find(key)
            else:
                print(f"Node {id} says: Not found {key}")
        elif id < hash <= succ or (hash > id > succ) or (id > succ >= hash):
            print(f"Node {id} says: Find from {id} to {succ}")
            id_node = succ
            stub = get_stub(f"127.0.0.1:500{CHORD.index(succ)}")
            find_data_response = stub.FindData(request)
            status = find_data_response.data
        else:
            cur = -1
            for i in range(len(finger_table) - 1):
                if finger_table[i] <= hash:
                    cur = max(cur, finger_table[i])
            if cur == -1:
                cur = max(finger_table)
            print(f"Node {id} says: Find from {id} to {cur}")
            stub = get_stub(f"127.0.0.1:500{CHORD.index(cur)}")
            find_data_response = stub.FindData(request)
            status = find_data_response.data
            id_node = find_data_response.node_id

        reply = {'data': status, 'node_id': id_node}
        return pb2.FindDataResponse(**reply)

    def GetFingerTable(self, request, context):
        global finger_table
        reply = {'id': finger_table}
        return pb2.GetFingerTableResponse(**reply)


if __name__ == "__main__":

    node_port = str(5000 + int(node_id))
    node = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ChordServicer_to_server(NodeHandler(), node)
    node.add_insecure_port("127.0.0.1:" + node_port)
    node.start()
    id = CHORD[int(node_id)]
    populate_finger_table(id)
    print(f"Node {id} finger table {finger_table}")

    try:
        node.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down")
