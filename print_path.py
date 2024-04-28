import codecs
import sys
import linecache
import os.path as osp

log_dir = sys.argv[1]
line_ids = [int(line_id) for line_id in sys.argv[2:]]
nodes = {}
for line in open(osp.join(log_dir, 'paths.node')).readlines():
    split = line.index(' ')
    node_id = line[:split]
    node_label = line[split+1:]
    nodes[node_id] = node_label

for line_id in line_ids:
    line = linecache.getline(osp.join(log_dir, 'paths.edge'), line_id)
    elements = line.split(' ')
    result = []
    for element in elements:
        if element.isnumeric() or (element[0] == '-' and element[1:].isnumeric()):
            label = codecs.decode(nodes[element], 'unicode_escape')
            label = element + '\n\t' + '\n\t'.join(label.split('\n'))
            result.append(label)
        else:
            result.append(element)
            
    print(f"========================= {line_id} =========================")
    for result_line in result:
        print(result_line)
    print('\n')