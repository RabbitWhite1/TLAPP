class ProtocolObject:
    def __init__(self):
        ...

class Extractor:
    def __init__(self):
        ...

    def extract(self, action, prev_state, cur_state):
        raise NotImplementedError('extract() is not implemented')