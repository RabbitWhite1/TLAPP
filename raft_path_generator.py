from lib import *
from json import JSONEncoder

def usage():
    sys.stdout.write("path_generator.py originally by Dong WANG. Adapted by protosim developers for our use case.\n")
    sys.stdout.write("    Generate all paths from a .dot file created by TLC checking output\n\n")
    sys.stdout.write("USAGE: path_generator.py END_ACTION /path/to/file.dot /path/to/store/paths\n [POR]")

class RaftEntry:
    term: int
    value: int

    def __init__(self):
        ...

    @staticmethod
    def parse(entries: str) -> 'RaftEntry':
        if entries == "" or entries is None:
            return None
        res = RaftEntry()
        matched = re.match(r"\[term\|->(\d+),value\|->(\d+)\]", entries)
        if matched is None:
            raise Exception(f"Error parsing entries: {entries}")
        res.term = int(matched.group(1))
        res.value = int(matched.group(2))
        return res

    def to_dict(self):
        return self.__dict__
    
    
class RaftMessage:
    mtype: str
    mterm: int
    mindex: int
    mindex_term: int
    msuccess: bool
    mentries: RaftEntry
    mcommit_index: str
    mmatch_index: int
    msource: int
    mdest: int

    count: int

    def __init__(self):
        ...

    @staticmethod
    def parse(message: str, count: int, hint=None) -> 'RaftMessage':
        res = RaftMessage()
        res.count = count
        if hint == "ClientRequest":
            res.mentries = RaftEntry.parse(message)
            res.mtype = "ClientRequest"
            return res
        if (matched := re.match(r"\[mtype\|->AppendEntriesRequest,mterm\|->(\d+),msource\|->(\d+),mdest\|->(\d+),mprevLogIndex\|->(\d+),mprevLogTerm\|->(\d+),mentries\|-><<(.*)>>,mlog\|-><<.*>>,mcommitIndex\|->(\d+)\]", message)) is not None:
            #                                                                1                2              3                      4                     5                  6                                      7
            # mentries format: [term|->1,value|->1]
            res.mtype = "AppendEntriesRequest"
            res.mterm = int(matched.group(1))
            res.msource = int(matched.group(2))
            res.mdest = int(matched.group(3))
            res.mindex = int(matched.group(4))
            res.mindex_term = int(matched.group(5))
            res.mentries = RaftEntry.parse(matched.group(6))
            res.mcommit_index = matched.group(7)
        elif (matched := re.match(r"\[mtype\|->RequestVoteRequest,mterm\|->(\d+),mlastLogTerm\|->(\d+),mlastLogIndex\|->(\d+),msource\|->(\d+),mdest\|->(\d+)\]", message)):
            #                                                               1                      2                      3                4              5
            res.mtype = "RequestVoteRequest"
            res.mterm = int(matched.group(1))
            res.mindex_term = int(matched.group(2))
            res.mindex = int(matched.group(3))
            res.msource = int(matched.group(4))
            res.mdest = int(matched.group(5))
        else:
            raise NotImplementedError(f"Parsing for {message=}, {hint=} is not implemented yet.")
        return res
    
    def to_dict(self):
        d = copy.deepcopy(self.__dict__)
        d['mentries'] = self.mentries.to_dict()
        return d
    

def get_messages_from_state(state, messages_name=None) -> dict[str, int]:
    if messages_name is None:
        messages_name = 'messages'
    messages = [variable for variable in codecs.decode(state, 'unicode_escape').split('/\\') if variable.startswith(f' {messages_name}')][0].replace('\n', '').replace(' ', '')
    if (matched := re.match(f'{messages_name}=<<>>', messages)) is not None:
        return {}
    elif (matched := re.match(f'{messages_name}=\((.*?)\)', messages)) is not None:
        messages = matched.group(1)
    else:
        raise Exception(f"Error parsing messages: {messages}")
    messages = messages.split('@@')
    messages = [message_count.split(":>") for message_count in messages]
    messages = {message_count[0]: int(message_count[1]) for message_count in messages}
    return messages

def get_messages_diff(prev_node_messages, node_messages) -> dict[str, int]:
    diff = copy.deepcopy(node_messages)
    for message in prev_node_messages:
        if message in diff:
            diff[message] = diff[message] - prev_node_messages[message]
            if diff[message] == 0:
                diff.pop(message)
        else:
            assert prev_node_messages[message] == 1 and "If a message is not found, it is used, but at most once."
            diff[message] = -1
    return diff

def get_log_from_state(state) -> list[str]:
    # Examples:
    # log = <<<<[term |-> 1, value |-> 1]>>, <<>>, <<>>>>
    # log = <<<<>>, <<>>, <<>>>>
    log = [variable for variable in codecs.decode(state, 'unicode_escape').split('/\\') if variable.startswith(' log')][0].replace('\n', '').replace(' ', '')
    if (matched:=re.match(r'log=<<(.+)>>', log)) is not None:
        log_by_node = re.findall(r'(<<.*?>>)', matched.group(1))
        return log_by_node
    else:
        raise Exception(f"Error parsing log: {log}") 
        
class RaftExtractor(Extractor):
    def __init__(self):
        ...
    
    # @override
    def extract(self, action, prev_state, cur_state) -> dict:
        # TODO: Generate ClientResponse for `AdvanceCommitIndex` action.
        if action == 'ClientRequest':
            prev_log = get_log_from_state(prev_state)
            cur_log = get_log_from_state(cur_state)
            diff_nodes = [(i, prev_log_i, cur_log_i) for i, (prev_log_i, cur_log_i) in enumerate(zip(prev_log, cur_log)) if prev_log_i != cur_log_i]
            if len(diff_nodes) != 1:
                raise Exception(f"Error parsing log diff: {prev_log=}, {cur_log=}, {diff_nodes=}")
            diff_node = diff_nodes[0]
            diff_node_id, diff_node_prev, diff_node_cur = diff_node
            diff_node_prev = diff_node_prev.strip('<>')
            diff_node_cur = diff_node_cur.strip('<>')
            if not diff_node_cur.startswith(diff_node_prev):
                raise Exception(f"Error parsing log diff: {diff_node_cur} does not start with {diff_node_prev}")
            diff = diff_node_cur[len(diff_node_prev):].strip(',')
            if not diff.startswith("[")or not diff.endswith("]"):
                raise Exception(f"Error parsing log diff: {diff=}")
            diff = [RaftMessage.parse(diff, 1, hint="ClientRequest")]
            diff[0].mdest = diff_node_id
            return {"action": action, "diff": diff}
        else:
            prev_node_messages = get_messages_from_state(prev_state)
            node_messages = get_messages_from_state(cur_state)
            diff = get_messages_diff(prev_node_messages, node_messages)
            diff = [RaftMessage.parse(message, count) for message, count in diff.items()]
            return {"action": action, "diff": diff}


if __name__ == '__main__':
    extractor = RaftExtractor()
    main(extractor)