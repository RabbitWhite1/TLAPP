import codecs
import copy
import re
import pyparsing as pp
from lib import *
from extractor import ProtocolObject
from toolz import partition


num_nodes = 3

class ZKProposal(ProtocolObject):
    def __init__(self):
        self.mzxid: int = None
        self.mreq_id: int = None

    @staticmethod
    def parse() -> "ZKProposal":
        raise NotImplementedError

class ZkMessage(ProtocolObject):
    def __init__(self):
        self.mtype: str = None
        self.msource: int = None
        self.mdest: int = None
        self.mzxid: int = None
        self.mepoch: int = None
        # Data itself is not important for consensus itself.

        self.mreq_id: int = None

        # For BIGSYNC
        self.mnew_leader_zxid: int = None
        self.mproposals: list[ZKProposal] = None
        self.mcommits: list[int] = None

        self.count: int = None

    @staticmethod
    def makeTCP(src: int, dst: int, count: int) -> 'ZkMessage':
        res = ZkMessage()
        res.msource = src
        res.mdest = dst
        res.mtype = "TCP"
        res.count = count
        return res
    
    @staticmethod
    def makeClientRequest(req_id: int, dst: int, count: int) -> 'ZkMessage':
        res = ZkMessage()
        res.mdest = dst
        res.mtype = "CREQ"
        res.count = count
        res.mreq_id = req_id
        return res
    
    @staticmethod
    def makeClientResponse(req_id: int, src: int, count: int) -> 'ZkMessage':
        res = ZkMessage()
        res.msource = src
        res.mtype = "CRESP"
        res.count = count
        res.mreq_id = req_id
        return res

    @staticmethod
    def parse(message: str, src: int, dst: int, count: int) -> 'ZkMessage':
        res = ZkMessage()
        res.msource = src
        res.mdest = dst
        res.count = count
        if (matched := re.match(r"mtype\|->FI,mzxid\|-><<(\d+),(\d+)>>", message)):
            # FOLLOWERINFO
            res.mtype = "FI"
            res.mzxid = (int(matched.group(1)) << 32) | int(matched.group(2))
        elif (matched := re.match(r"mtype\|->LI,mzxid\|-><<(\d+),(\d+)>>", message)):
            # LEADERINFO
            res.mtype = "LI"
            res.mzxid = (int(matched.group(1)) << 32) | int(matched.group(2))
        elif (matched := re.match(r"mtype\|->AE,mzxid\|-><<(\d+),(\d+)>>,mepoch\|->([-\d]+)", message)):
            # ACKEPOCH
            res.mtype = "AE"
            res.mzxid = (int(matched.group(1)) << 32) | int(matched.group(2))
            res.mepoch = int(matched.group(2))
        elif (matched := re.match(r"mtype\|->DF,mzxid\|-><<(\d+),(\d+)>>", message)):
            # DIFF
            res.mtype = "DF"
            res.mzxid = (int(matched.group(1)) << 32) | int(matched.group(2))
        elif (matched := re.match(r"mtype\|->TC,mtruncZxid\|-><<(\d+),(\d+)>>", message)):
            # TRUNC
            res.mtype = "TC"
            res.mzxid = (int(matched.group(1)) << 32) | int(matched.group(2))
        elif (matched := re.match(r"mtype\|->PP,mzxid\|-><<(\d+),(\d+)>>,mdata\|->(\d+)", message)):
            # PROPOSAL
            res.mtype = "PP"
            res.mzxid = (int(matched.group(1)) << 32) | int(matched.group(2))
            res.mreq_id = int(matched.group(3))
        elif (matched := re.match(r"mtype\|->CT,mzxid\|-><<(\d+),(\d+)>>", message)):
            # COMMIT
            res.mtype = "CT"
            res.mzxid = (int(matched.group(1)) << 32) | int(matched.group(2))
        elif (matched := re.match(r"mtype\|->NL,mzxid\|-><<(\d+),(\d+)>>", message)):
            # NEWLEADER
            res.mtype = "NL"
            res.mzxid = (int(matched.group(1)) << 32) | int(matched.group(2))
        elif (matched := re.match(r"mtype\|->AL,mzxid\|-><<(\d+),(\d+)>>", message)):
            # ACKLD
            res.mtype = "AL"
            res.mzxid = (int(matched.group(1)) << 32) | int(matched.group(2))
        elif (matched := re.match(r"mtype\|->AK,mzxid\|-><<(\d+),(\d+)>>", message)):
            # ACK
            res.mtype = "AK"
            res.mzxid = (int(matched.group(1)) << 32) | int(matched.group(2))
        else:
            raise ValueError(f"Unknown message type: {message}")
        return res
    
    def to_dict(self):
        d = {k: v for k, v in self.__dict__.items() if v is not None}
        return d

pp_message = pp.Suppress("[") + pp.Word(pp.srange(r"[a-zA-Z0-9_|\->,<>{}]")) + pp.Suppress("]")
pp_channel = pp.Suppress('<<') + pp.Group(pp.Empty() ^ pp.delimitedList(pp_message, ',')) + pp.Suppress('>>')
pp_node_channels = pp.Suppress('<<') + pp.Group(pp.Empty() ^ pp.delimitedList(pp_channel, ',')) + pp.Suppress('>>')
pp_all_nodes_channels = pp.Suppress('<<') + pp.Group(pp.Empty() ^ pp.delimitedList(pp_node_channels, ',')) + pp.Suppress('>>')

def get_nested_parser(num_elements_per_layer, left='<<', right='>>'):
    pattern = r'<<(.*)>>'
    for num_elements in reversed(num_elements_per_layer):
        pattern = "<<" + ','.join([pattern] * num_elements) + ">>"
    total = 1
    for num_elements in num_elements_per_layer:
        total *= num_elements
    def parser(s):
        res = re.match(pattern, s).groups()
        res = [[item.strip('[]') for item in re.findall(r'\[.*?\]', res_i)] for res_i in res]
        for num_elements in reversed(num_elements_per_layer):
            res = list(partition(num_elements, res))
        return res

    return parser

def pp_channel_parser(s):
    return [[item.strip('[]') for item in re.findall(r'\[.*?\]', s)]]
pp_node_channels_parser = get_nested_parser([num_nodes])
pp_all_nodes_channels_parser = get_nested_parser([num_nodes, num_nodes])


def get_messages_from_state(state) -> list[list[list[str]]]:
    messages = [variable for variable in codecs.decode(state, 'unicode_escape').split('/\\') if variable.startswith(f' msgs')][0].replace('\n', '').replace(' ', '')
    messages = messages.lstrip('msgs=')
    messages = pp_all_nodes_channels_parser(messages)[0]
    return messages

def get_parsed_messages_diff(prev_node_messages, node_messages) -> list[ZkMessage]:
    diffs_recv = []
    diffs_send = []
    for i, (prev_node, node) in enumerate(zip(prev_node_messages, node_messages)):
        for j, (prev_channel, channel) in enumerate(zip(prev_node, node)):
            if len(prev_channel) < len(channel):
                # remove same prefix
                prefix = 0
                for prefix in range(len(prev_channel)):
                    if prev_channel[prefix] != channel[prefix]:
                        break
                new_messages = channel[prefix:]
                for message in new_messages:
                    diffs_send.append(ZkMessage.parse(message, i+1, j+1, count=1))
            elif len(prev_channel) > len(channel):
                # remove suffix
                suffix = len(prev_channel)-1
                for suffix in range(len(prev_channel)-1, len(channel), -1):
                    if prev_channel[suffix] != channel[suffix]:
                        break
                used_messages = prev_channel[:suffix+1]
                for message in used_messages:
                    diffs_recv.append(ZkMessage.parse(message, i+1, j+1, count=-1))
            else:
                if prev_channel != channel:
                    print(f"Diff in node {i}, channel {j}")
                    print(f"Prev: {prev_channel}")
                    print(f"New: {channel}")
                    raise RuntimeError("Unexpected message diff")
    return diffs_recv + diffs_send

def get_hisotry_from_state(state) -> list[list[str]]:
    history = [variable for variable in codecs.decode(state, 'unicode_escape').split('/\\') if variable.startswith(f' history')][0].replace('\n', '').replace(' ', '')
    history = history.lstrip('history=')
    # history = pp_node_channels.parse_string(history).as_list()[0]
    history = pp_node_channels_parser(history)[0]
    return history

def get_history_diff_req_id(prev_history, cur_history) -> tuple[int, int]:
    req_id = None
    dest = None
    for i, (prev_node, node) in enumerate(zip(prev_history, cur_history)):
        if len(prev_node) != len(node):
            assert req_id is None
            assert len(node) == len(prev_node) + 1
            for i in range(len(prev_node)):
                assert prev_node[i] == node[i]
            diff = re.match(r'zxid\|-><<\d+,\d+>>,value\|->(\d+),ackSid\|->\{.*\},epoch\|->\d+', node[-1])
            req_id = int(diff.group(1))
            dest = i+1
    return dest, req_id

def get_last_committed(prev_state) -> list[int]:
    last_committed = [variable for variable in codecs.decode(prev_state, 'unicode_escape').split('/\\') if variable.startswith(f' lastCommitted')][0].replace('\n', '').replace(' ', '').lstrip('lastCommitted=')
    # last_committed = pp_channel.parse_string(last_committed).as_list()[0]
    last_committed = pp_channel_parser(last_committed)[0]
    last_committed = [int(re.match(r'zxid\|-><<\d+,\d+>>,index\|->(\d+)', last_committed_i).group(1)) for last_committed_i in last_committed]
    return last_committed

def get_last_committed_diff(prev_state, cur_state) -> tuple[int, int]:
    prev_last_committed = get_last_committed(prev_state)
    cur_last_committed = get_last_committed(cur_state)
    index = None
    src = None
    for i, (prev_committed, committed) in enumerate(zip(prev_last_committed, cur_last_committed)):
        if prev_committed != committed:
            assert index is None
            assert prev_committed + 1 == committed
            index = int(committed)
            src = i+1
    return index, src

def get_req_id(state, index, src):
    history = [variable for variable in codecs.decode(state, 'unicode_escape').split('/\\') if variable.startswith(f' history')][0].replace('\n', '').replace(' ', '').lstrip('history=')
    history = pp_node_channels_parser(history)[0][src-1]
    req_id = int(re.match(r'zxid\|-><<\d+,\d+>>,value\|->(\d+),ackSid\|->\{.*\},epoch\|->\d+', history[index-1]).group(1))
    return req_id

class ZKExtractor(Extractor):
    def __init__(self):
        ...
    
    # @override
    def extract(self, action, prev_state, cur_state) -> dict:
        # TODO: Generate ClientResponse for `AdvanceCommitIndex` action.
        if action == 'LeaderProcessRequest':
            prev_history = get_hisotry_from_state(prev_state)
            cur_history = get_hisotry_from_state(cur_state)
            dst, req_id = get_history_diff_req_id(prev_history, cur_history)
            return {"action": action, "diff": [ZkMessage.makeClientRequest(req_id, dst=dst, count=1)]}
        elif action == 'LeaderProcessACK':
            committed, src = get_last_committed_diff(prev_state, cur_state)
            if committed is None:
                return {"action": action, "diff": []}
            else:
                req_id = get_req_id(cur_state, committed, src)
                return {"action": action, "diff": [ZkMessage.makeClientResponse(req_id, src=src, count=-1)]}
        else:
            prev_node_messages = get_messages_from_state(prev_state)
            node_messages = get_messages_from_state(cur_state)
            diffs = get_parsed_messages_diff(prev_node_messages, node_messages)
            if action == 'ConnectAndFollowerSendFOLLOWERINFO':
                # TODO: prepend TCP message for this action
                assert len(diffs) == 1 and diffs[0].count == 1 and diffs[0].mtype == "FI"
                follower_info = diffs[0]
                tcps = [
                    ZkMessage.makeTCP(follower_info.msource, follower_info.mdest, count=1), 
                    ZkMessage.makeTCP(follower_info.msource, follower_info.mdest, count=-1), 
                    ZkMessage.makeTCP(follower_info.mdest, follower_info.msource, count=1),
                    ZkMessage.makeTCP(follower_info.mdest, follower_info.msource, count=-1),
                ]
                diffs = tcps + diffs
            elif action == '':
                ...
            # if len(diffs) > 0:
            #     print(diffs)
            return {"action": action, "diff": diffs}


if __name__ == '__main__':
    extractor = ZKExtractor()
    main(extractor)