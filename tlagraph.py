import re
import subprocess
import rich
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, MofNCompleteColumn
from tqdm import tqdm

class Node:
    def __init__(self, node_id: int, label=None):
        self.node_id = node_id
        self.label = label

class Edge:
    def __init__(self, src: Node, dst: Node, label: str):
        self.src = src
        self.dst = dst
        self.label = label

class AdjacencyList:
    def __init__(self, node: Node):
        self.node = node
        self.edges: list[Edge] = []
        self.edges_map: dict[int, Edge] = {}

    def add_edge(self, edge: Edge):
        self.edges.append(edge)
        self.edges_map[edge.dst.node_id] = edge

    def get_edge(self, dst_id: int) -> Edge:
        return self.edges_map[dst_id]

class TLAGraph:
    def __init__(self):
        self.node_ids: list[int] = []
        self.adjacency: dict[int, AdjacencyList] = {}
        self.root_id = None

    @staticmethod
    def from_file(dot_file_path: str) -> 'TLAGraph':
        graph = TLAGraph()
        num_lines = int(subprocess.check_output(['wc', '-l', dot_file_path]).decode('utf-8').strip(' \t\r\n').split(' ')[0])
        print(f'Dot file has {num_lines} lines')
        with open(dot_file_path) as dot_file:
            with Progress(TextColumn("Parsing dot file"), BarColumn(), MofNCompleteColumn(),
                          TimeRemainingColumn(), TimeElapsedColumn()) as progress:
                for _ in progress.track(range(num_lines)):
                    line = dot_file.readline()
                    if line == '':
                        break
                    # print(line)
                    if (edge_match := re.match(r'([-\d]+) -> ([-\d]+) \[label="(.*?)",', line)):
                        src_id = int(edge_match.group(1))
                        dst_id = int(edge_match.group(2))
                        label = edge_match.group(3)
                        graph.add_edge(src_id, dst_id, label)
                    elif (node_match := re.match(r'([-\d]+) \[label="(.*?)"', line)):
                        node_id = int(node_match.group(1))
                        label = node_match.group(2)
                        graph.add_node(node_id, label)
                    elif (root_match := re.match(r'\{rank = same; ([-\d]+);\}', line)):
                        graph.root_id = int(root_match.group(1))
        return graph
    
    def number_of_nodes(self) -> int:
        return len(self.node_ids)
    
    def number_of_edges(self) -> int:
        return sum([len(adjacency.edges) for adjacency in self.adjacency.values()])

    def has_node(self, node_id: int) -> bool:
        return node_id in self.adjacency

    def get_node(self, node_id: int) -> Node:
        return self.adjacency[node_id].node
    
    def get_edge(self, src_id: int, dst_id: int) -> Edge:
        return self.adjacency[src_id].get_edge(dst_id)
    
    def add_node(self, node_id: int, label: str) -> Node:
        if self.has_node(node_id):
            if self.get_node(node_id).label is not None:
                raise RuntimeError(f"Node {node_id} with non-None label already exists")
            else:
                node = self.get_node(node_id)
                node.label = label
                return node
        else:
            self.node_ids.append(node_id)
            node = Node(node_id, label)
            self.adjacency[node_id] = AdjacencyList(node)
            return node
    
    def add_edge(self, src_id: str, dst_id: str, label: str) -> Edge:
        src_node = self.adjacency[src_id].node
        if dst_id not in self.adjacency:
            self.add_node(dst_id, label=None)
        dst_node = self.adjacency[dst_id].node
        edge = Edge(src_node, dst_node, label)
        self.adjacency[src_id].add_edge(edge)
        return edge

    def nodes(self) -> list[int]:
        return self.node_ids

    def successors(self, node_id: int) -> list[int]:
        return [edge.dst.node_id for edge in self.adjacency[node_id].edges]

    def successor_edges(self, node_id: int) -> list[Edge]:
        return self.adjacency[node_id].edges
    
    def num_successors(self, node_id: int) -> int:
        return len(self.adjacency[node_id].edges)
    