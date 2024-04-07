
import json
import networkx as nx
import sys
from extractor import Extractor
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn, TimeElapsedColumn


def find_root(graph):
    for n in graph.nodes(data=True):
        predecessors = graph.predecessors(n[0])
        if len(list(predecessors)) == 0 and len(list(graph.successors(n[0]))) != 0:
            return n

def get_num_leaves(graph):
    count = 0
    for n in graph.nodes(data=True):
        if len(list(graph.predecessors(n[0]))) != 0 and len(list(graph.successors(n[0]))) == 0:
            count += 1
    return count

def output_edge_and_path(graph, path, edge_file, message_file, extractor: Extractor):
    label = nx.get_node_attributes(graph, 'label')
    prev_node = None
    diffs = []
    for i, node in enumerate(path):
        if i == 0:
            edge_file.write(node + ' ')
            prev_node = node
        else:
            action = graph[path[i-1]][node]['label']
            edge_file.write(action + ' ' + node + ' ')
            # write the diff of two nodes to message file
            if prev_node != None:
                diffs.append(extractor.extract(action, label[prev_node], label[node]))
            prev_node = node
    edge_file.write('\n')
    message_file.write(json.dumps(diffs, default=lambda o: o.__dict__) + '\n')

def dfs_and_output(graph, edge_file, message_file, extractor: Extractor):
    edges = nx.edges(graph)
    for edge in edges:
        graph[edge[0]][edge[1]]['visited'] = False
        
    root = find_root(graph)
    num_leaves = get_num_leaves(graph)
    path = [(root[0], graph.successors(root[0]))]
    count = 0
    progress = Progress(TextColumn("Path Writing"), BarColumn(), 
                        TextColumn("{task.completed}"),
                        TimeElapsedColumn())
    # TODO: Didn't use the progress bar for now, hard to compute the total number of paths
    task_id = progress.add_task("Path Writing", total=None)
    with progress:
        while len(path) > 0:
            node_id, succ_iter = path[-1]
            if len(list(graph.successors(node_id))) == 0:
                count += 1
                path_node_ids = [node for node, _ in path]
                output_edge_and_path(graph, path_node_ids, edge_file, message_file, extractor)
                path.pop()
                progress.advance(task_id)
            else:
                try:
                    next_node_id = next(succ_iter)
                    if not graph[node_id][next_node_id]['visited']:
                        graph[node_id][next_node_id]['visited'] = True
                        path.append((next_node_id, graph.successors(next_node_id)))
                except StopIteration:
                    path.pop()

def main(extractor):
    if len(sys.argv) != 3:
        print('Usage: python dfs.py <state.dot> <output>')
        sys.exit(1)

    dot_file_path = sys.argv[1]
    output = sys.argv[2]
    
    with (progress:=Progress(TextColumn("Graph Loading"), BarColumn(), TimeElapsedColumn())):
        graph = nx.DiGraph(nx.drawing.nx_agraph.read_dot(dot_file_path))
    num_nodes = nx.DiGraph.number_of_nodes(graph)
    num_edges = nx.DiGraph.number_of_edges(graph)
    print(f'Number of nodes: {num_nodes}')
    print(f'Number of edges: {num_edges}')

    label = nx.get_node_attributes(graph, 'label')
    node_file = open(f'{output}.node', 'w')
    # Write nodes
    with (progress:=Progress(TextColumn("Node Writing"), BarColumn(), 
                             MofNCompleteColumn(),
                             TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), 
                             TimeRemainingColumn(), TimeElapsedColumn())):
        for node in progress.track(graph, total=nx.DiGraph.number_of_nodes(graph)):
            if node.isdigit() or node.startswith('-'):
                node_file.write(node + ' ' + label[node] + '\n')
    
    # Write paths
    edge_file = open(f'{output}.edge', 'w')
    message_file = open(f'{output}.message', 'w')
    dfs_and_output(graph, edge_file, message_file, extractor)
    