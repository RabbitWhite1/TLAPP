import codecs
import copy
import re
from lib import *

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
    mentries: list[RaftEntry]
    mcommit_index: str
    mmatch_index: int
    msource: int
    mdest: int

    mreq_id: int

    count: int

    def __init__(self):
        ...

    @staticmethod
    def parse(message: str|int, count: int, hint=None, **kwargs) -> 'RaftMessage':
        res = RaftMessage()
        res.count = count
        if hint == "ClientRequest":
            entry = RaftEntry.parse(message)
            res.mentries = [entry] # Keep the entry for easy debugging
            res.mreq_id = entry.value
            res.mtype = "ClientRequest"
            res.mdest = kwargs['dest']
            return res
        elif hint == "ClientResponse":
            res.mreq_id = message
            res.mtype = "ClientResponse"
            res.msource = kwargs['source']
            return res
        elif (matched := re.match(r"\[mtype\|->AE,mterm\|->(\d+),msource\|->(\d+),mdest\|->(\d+),mprevLogIndex\|->(\d+),mprevLogTerm\|->(\d+),mentries\|-><<(.*)>>,mlog\|-><<.*>>,mcommitIndex\|->(\d+)\]", message)) is not None:
            #                                                                1                2              3                      4                     5                  6                                      7
            # mentries format: [term|->1,value|->1]
            res.mtype = "AppendEntriesRequest"
            res.mterm = int(matched.group(1))
            res.msource = int(matched.group(2))
            res.mdest = int(matched.group(3))
            res.mindex = int(matched.group(4))
            res.mindex_term = int(matched.group(5))
            res.mentries = RaftEntry.parse(matched.group(6))
            if res.mentries is None:
                res.mentries = []
            else:
                res.mentries = [res.mentries]
            res.mcommit_index = int(matched.group(7))
        elif (matched := re.match(r"\[mtype\|->AER,mterm\|->(\d+),msource\|->(\d+),mdest\|->(\d+),msuccess\|->(TRUE|FALSE),mmatchIndex\|->(\d+)\]", message)) is not None:
            #                                                                   1                 2              3                4                    5
            res.mtype = "AppendEntriesResponse"
            res.mterm = int(matched.group(1))
            res.msource = int(matched.group(2))
            res.mdest = int(matched.group(3))
            res.msuccess = True if (matched.group(4)) == "TRUE" else False
            res.mmatch_index = int(matched.group(5))
        elif (matched := re.match(r"\[mtype\|->RV,mterm\|->(\d+),mlastLogTerm\|->(\d+),mlastLogIndex\|->(\d+),msource\|->(\d+),mdest\|->(\d+)\]", message)) is not None:
            #                                                               1                      2                      3                4              5
            res.mtype = "RequestVoteRequest"
            res.mterm = int(matched.group(1))
            res.mindex_term = int(matched.group(2))
            res.mindex = int(matched.group(3))
            res.msource = int(matched.group(4))
            res.mdest = int(matched.group(5))
        elif (matched := re.match(r"\[mtype\|->RVR,mterm\|->(\d+),msource\|->(\d+),mdest\|->(\d+),mlog|-><<.*>>,mvoteGranted\|->(TRUE|FALSE)\]", message)) is not None:
            #                                                                 1                 2             3                                      4
            res.mtype = "RequestVoteResponse"
            res.mterm = int(matched.group(1))
            res.msource = int(matched.group(2))
            res.mdest = int(matched.group(3))
            res.msuccess = True if (matched.group(4)) == "TRUE" else False
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
    
def get_responsed_client_requests_from_state(state) -> list[list[int]]:
    responsed_client_requests = [variable for variable in codecs.decode(state, 'unicode_escape').split('/\\') if variable.startswith(' responsedClientRequests')][0].replace('\n', '').replace(' ', '')
    if (matched:=re.match(r'responsedClientRequests=<<(.+)>>', responsed_client_requests)) is not None:
        responsed_by_node = re.findall(r'\{(.*?)\}', matched.group(1))
        responsed_by_node = [[int(i) for i in r.split(',') if i != ''] for r in responsed_by_node]
        return responsed_by_node
    else:
        raise Exception(f"Error parsing responsedClientRequests: {responsed_client_requests}")
        
class RaftExtractor(Extractor):
    def __init__(self):
        ...
    
    # @override
    def extract(self, action, prev_state, cur_state) -> dict:
        # TODO: Generate ClientResponse for `AdvanceCommitIndex` action.
        if action == 'ClientRequest':
            prev_log = get_log_from_state(prev_state)
            cur_log = get_log_from_state(cur_state)
            diff_nodes = [(i+1, prev_log_i, cur_log_i) for i, (prev_log_i, cur_log_i) in enumerate(zip(prev_log, cur_log)) if prev_log_i != cur_log_i]
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
            diff = [RaftMessage.parse(diff, -1, hint="ClientRequest", dest=diff_node_id)]
            diff[0].mdest = diff_node_id
            return {"action": action, "diff": diff}
        elif action == 'AdvanceCommitIndex':
            prev_responsed = get_responsed_client_requests_from_state(prev_state)
            cur_responsed = get_responsed_client_requests_from_state(cur_state)
            # TODO: this might alter the order
            diffs = []
            for i, (cur_i, prev_i) in enumerate(zip(cur_responsed, prev_responsed)):
                assert len(cur_i) >= len(prev_i)
                if len(cur_i) == len(prev_i):
                    assert cur_i == prev_i
                    continue
                else:
                    diff = [RaftMessage.parse(req_id, 1, hint="ClientResponse", source=i+1) for req_id in cur_i if req_id not in prev_i and req_id != 0]
                    diffs.extend(diff)
            return {"action": action, "diff": diffs}
        else:
            prev_node_messages = get_messages_from_state(prev_state)
            node_messages = get_messages_from_state(cur_state)
            diffs = get_messages_diff(prev_node_messages, node_messages)
            diffs = [RaftMessage.parse(message, count) for message, count in diffs.items()]
            return {"action": action, "diff": diffs}


if __name__ == '__main__':
    extractor = RaftExtractor()
    main(extractor)