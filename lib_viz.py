import codecs
import copy
import json
import random
import re
import rich
import sys
import time

import rich.progress
from extractor import Extractor
import pygraphviz as pgv
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn

PATHS = [[]]
        

def usage():
    sys.stdout.write("path_generator.py originally by Dong WANG. Adapted by protosim developers for our use case.\n")
    sys.stdout.write("    Generate all paths from a .dot file created by TLC checking output\n\n")
    sys.stdout.write("USAGE: path_generator.py END_ACTION /path/to/file.dot /path/to/store/paths\n [POR]")
        
##################################### General functions #########################################

def find_root(graph):
    for n in graph.nodes():
        predecessors = graph.predecessors(n)
        if len(list(predecessors)) == 0 and len(list(graph.successors(n))) != 0:
            return n


"""
Output paths to files, in which '.node' file stores maps of 
state ID and contents, and '.edge' file stores paths.

Parameters:
    graph: the state space networkx graph
    output: the directory to store path files
Return:
    None
"""
def output(graph, output, extractor):
    get_node_label = lambda node: graph.get_node(node).attr['label']
    node_file = open(output+'.node','w')
    edge_file = open(output+'.edge','w')
    message_file = open(output+'.message','w')
    # Write all node information
    for i, node in enumerate(graph):
        if node.isdigit() or node.startswith('-'):
            node_file.write(node + ' ' + get_node_label(node) + '\n')
    # Write all edge information
    with (progress:=Progress(TextColumn("Path Writing"), BarColumn(), 
                             TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), 
                             TimeRemainingColumn(), TimeElapsedColumn())):
        for path_id, path in progress.track(enumerate(PATHS), total=len(PATHS)):
            prev_node = None
            diffs = []
            for i, node in enumerate(path):
                if i == 0:
                    edge_file.write(node + ' ')
                    prev_node = node
                else:
                    action = graph.get_edge(path[i-1], node).attr['label']
                    edge_file.write(action + ' ' + node + ' ')
                    # write the diff of two nodes to message file
                    if prev_node != None:
                        diffs.append(extractor.extract(action, get_node_label(prev_node), get_node_label(node)))
                    prev_node = node
            edge_file.write('\n')
            message_file.write(json.dumps(diffs, default=lambda o: o.__dict__) + '\n')




##################################### Basic Mocket functions #########################################

def add_node_visited(graph: pgv.AGraph):
    with (progress:=Progress(TextColumn("Labeling Node"), BarColumn(), 
                             TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), 
                             TimeRemainingColumn(), TimeElapsedColumn())):
        for node in progress.track(graph.nodes()):
            graph.get_node(node).attr['visited'] = '0'

dfs_path_count = 0
def add_visited_label_dfs_track(graph: pgv.AGraph, source: pgv.Node):
    progress = Progress(TextColumn("Labeling Node"), BarColumn(), 
                        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), 
                        TimeRemainingColumn(), TimeElapsedColumn())
    task_id = progress.add_task("Path Writing", total=graph.number_of_nodes())
    advancer = lambda : progress.advance(task_id)
    with progress:
        add_visit_label_dfs(graph, source, advancer)

def add_visit_label_dfs(graph: pgv.AGraph, source: pgv.Node, advancer):
    global dfs_path_count
    graph.get_node(source).attr['visited'] = '1'
    advancer()
    continued = False
    for child in graph.successors(source):
        child_node = graph.get_node(child)
        if child_node.attr['visited'] == '0':
            continued = True
            add_visit_label_dfs(graph, child, advancer)
        edge = graph.get_edge(source, child)
        edge.attr['visited'] = '0'
        edge.attr['por'] = '0'
    if continued == False:
        dfs_path_count += 1

"""
Partial order reduction. 
Add labels for edges in the rhombus structure. For two
commutative edges, we randomly choose one schedule and
set the label of edges in this schedule as equivalent,
and the labeled edges will not be added in any path in
traversal.

Specifically, we traverse every node to its grandchild
nodes to find these rhombuses.
     n0
    /  \ 
   e1   e2 
  /      \ 
n1        n2
  \      /  \ 
   e2   e1   e3
    \  /
     n3
Assume a rhombuse as above, we mark < n0 -> n1, n1 -> n3> 
or < n0 -> n2, n2 -> n3 > as equivalent. Note that n2 has
another grandchild. Thus, if we choose to set the path of 
n2 as equivalent, we should only skip n0 -> n2, and leave 
n2 -> n3 to cover e3.
     
Parameters:
    diGraph: the state space networkx graph

Returns:
    None
"""
def add_POR_label(diGraph: pgv.AGraph):
    left_or_right = ["L", "R"]
    for n in diGraph.nodes():
        top_node = n
        left_edge = None
        for left_node in diGraph.successors(top_node):
            left_edge = diGraph.get_edge(top_node, left_node).attr['label']
            for right_node in diGraph.successors(top_node):
                if left_node == right_node:
                    continue
                right_edge = diGraph.get_edge(top_node, right_node).attr['label']
                for btmNode in diGraph.successors(left_node):
                    if diGraph.has_edge(right_node, btmNode) and \
                        diGraph.get_edge(left_node, btmNode).attr['label'] == right_edge and \
                        diGraph.get_edge(right_node, btmNode).attr['label'] == left_edge:
                        # If these two paths has already been labeled, skip them.
                        if diGraph.get_edge(right_node, btmNode).attr['por'] == '1' or \
                            diGraph.get_edge(left_node, btmNode).attr['por'] == '1':
                            continue
                        # Randomly choose right path
                        elif random.choice(left_or_right) == "R":
                            diGraph.get_edge(left_node, btmNode).attr['por'] = '1'
                            if diGraph.out_degree(left_node) == 1:
                                diGraph.get_edge(top_node, left_node).attr['por'] = '1'
                        # Randomly choose left path
                        else:
                            diGraph.get_edge(right_node, btmNode).attr['por'] = '1'
                            if diGraph.out_degree(right_node) == 1:
                                diGraph.get_edge(top_node, right_node).attr['por'] = '1'

"""
Construct a path by edge-based traversal.
A path struct: (StateID1, StateID2, ...)

Parameters:
    diGraph: the state space networkx graph
    preNode: the root node
    curNode: current state
    end: end action name
    pathID: current path to generate
    POR: whether enable partial order reduction

Returns:
    None
"""
def traverse(diGraph, preNode, curNode, endAction, pathID, POR):
    global PATHS
    isEndState = False
    isAllOutEdgesVisited = True
    if(preNode != None): # Current node is not initial state
        actionName = diGraph.get_edge(preNode, curNode).attr['label']
        if(actionName == endAction):
            #print('Is end state:', curNode)
            isEndState = True
    for succ in diGraph.successors(curNode):
        if(diGraph.get_edge(curNode, succ).attr['visited'] == '0'):
            if (not POR) or (diGraph.get_edge(curNode, succ).attr['por'] == '0'):
                isAllOutEdgesVisited = False
                break
    if(isEndState or isAllOutEdgesVisited):
        return
    first_succ = None
    for succ in diGraph.successors(curNode):
        if(diGraph.get_edge(curNode, succ).attr['visited'] == '1') or (POR and (diGraph.get_edge(curNode, succ).attr['por'] == '1')):
            #print('Is visited edge:',curNode, succ)
            continue
        else:
            diGraph.get_edge(curNode, succ).attr['visited'] = '1'
            # If it is the first successor, we directly
            # add it in the path
            if first_succ == None:
                first_succ = succ
                PATHS[pathID].append(succ)
                #print('Add new node:', succ)
            # If it is not the first, we generate a new
            # path for later traversal
            else:
                path_num = len(PATHS)
                new_path = PATHS[pathID].copy()
                new_path.pop()
                PATHS.append(new_path)
                PATHS[path_num].append(succ)
                #print('Add new path for:', succ)
    if first_succ != None:
        #print('Traverse at:', first_succ)
        traverse(diGraph, curNode, first_succ, endAction, pathID, POR)


def add_node_visited(graph: pgv.AGraph):
    with (progress:=Progress(TextColumn("Labeling Node"), BarColumn(), 
                             TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), 
                             TimeRemainingColumn(), TimeElapsedColumn())):
        for node in progress.track(graph.nodes()):
            graph.get_node(node).attr['visited'] = '0'


def add_visit_label_dfs(graph: pgv.AGraph, source: pgv.Node, advancer):
    global dfs_path_count
    graph.get_node(source).attr['visited'] = '1'
    advancer()
    continued = False
    for child in graph.successors(source):
        child_node = graph.get_node(child)
        if child_node.attr['visited'] == '0':
            continued = True
            add_visit_label_dfs(graph, child, advancer)
        edge = graph.get_edge(source, child)
        edge.attr['visited'] = '0'
        edge.attr['por'] = '0'
    if continued == False:
        dfs_path_count += 1

class PathFinder:
    def __init__(self, graph: pgv.AGraph, step_limit: int, extractor: Extractor, output_prefix: str):
        self.graph = graph
        self.step_limit = step_limit
        self.extractor = extractor
        self.output_prefix = output_prefix

        self.node_file = open(output_prefix + '.node', 'w')
        self.edge_file = open(output_prefix + '.edge', 'w')
        self.message_file = open(output_prefix + '.message', 'w')
        self.count = 0

    def get_node_label(self, node_id):
        return self.graph.get_node(node_id).attr['label']
        
    def write_all_nodes(self):
        for i, node in enumerate(self.graph):
            if node.isdigit() or node.startswith('-'):
                self.node_file.write(node + ' ' + self.get_node_label(node) + '\n')

    
    def step_limit_dfs(self, source: str, path: list[str], advancer):
        graph = self.graph
        if len(path)-1 >= self.step_limit or len(graph.successors(source)) == 0:
            self.count += 1
            advancer()
            self.write_one_path(path)
            return
        for child in graph.successors(source):
            path.append(child)
            self.step_limit_dfs(child, path, advancer)
            path.pop()

    def step_limit_dfs_track(self, source: str):
        progress = Progress(TextColumn("DFS Writing Paths"), BarColumn(), 
                            TextColumn("#of paths: {task.completed}"), 
                            TimeElapsedColumn())
        task_id = progress.add_task("Path Writing", total=None)
        advancer = lambda : progress.advance(task_id)
        with progress:
            # for i in progress.track(range(10000000)):
                # advancer()
            self.step_limit_dfs(source, [source], advancer)

    def write_one_path(self, path: list[str]):
        graph = self.graph
        prev_node = None
        diffs = []
        for i, node in enumerate(path):
            if i == 0:
                self.edge_file.write(node + ' ')
                prev_node = node
            else:
                action = graph.get_edge(path[i-1], node).attr['label']
                self.edge_file.write(action + ' ' + node + ' ')
                # write the diff of two nodes to message file
                if prev_node != None:
                    diffs.append(self.extractor.extract(action, self.get_node_label(prev_node), self.get_node_label(node)))
                prev_node = node
        self.edge_file.write('\n')
        self.message_file.write(json.dumps(diffs, default=lambda o: o.__dict__) + '\n')

"""
Main function
"""
def main(extractor: Extractor):
    if (len(sys.argv) > 3):
        end = "END_ACTION"
        path = sys.argv[1]
        dir = sys.argv[2]
        step_limit = int(sys.argv[3])
        POR = False
        if (len(sys.argv) == 5):
            if (sys.argv[4].lower() == 'por'):
                POR = True
    else:
        usage()
        sys.exit(1)
    
    try:
        open(path)
    except IOError as e:
        sys.stderr.write("ERROR: could not read file" + path + "\n")
        usage()
        sys.exit(1)

    start_time = time.time()
    graph = pgv.AGraph(path)
    print(f"Successfully read graph file in {time.time() - start_time}")

    n = graph.number_of_nodes()
    e = graph.number_of_edges()
    print("The graph contains", n, "nodes and", e, "edges.")

    root = find_root(graph)
    print("Found the root node:", root)

    path_finder = PathFinder(graph, step_limit, extractor, dir)
    path_finder.write_all_nodes()
    path_finder.step_limit_dfs_track(root)

    # add_node_visited(graph)
    # print('Done label node')
    # add_visited_label_dfs_track(graph, root)
    # print('Done label visit')
    # # if POR:
    # #     add_POR_label(graph)
    # #     print("Done label POR")

    # PATHS[0].append(root)
    # traverse(graph, None, root, end, 0, POR)
    # path_num = len(PATHS)
    # print('path[ 0 ]:', 'path num:', path_num, ',length', len(PATHS[0]))
    

    # cur_path_ID = 1
    # cur_path_num = len(PATHS)
    # while cur_path_ID < cur_path_num:
    #     cur_path = PATHS[cur_path_ID]
    #     length = len(cur_path)
    #     if length > 1: # The path should at least contain an initial state and current state
    #         traverse(graph, cur_path[length-2], cur_path[length-1], end, cur_path_ID, POR)
    #     else:
    #         sys.stderr.write("ERROR! Wrong traversal for current path!")
    #     cur_path_num = len(PATHS)
    #     print('path[', cur_path_ID, '],', 'path num:', cur_path_num, ',length', len(PATHS[cur_path_ID]))
    #     cur_path_ID = cur_path_ID + 1

    # output(graph, dir, extractor)
    # print("--- %s seconds spent---" % (time.time() - start_time))