import json


with open('porto/dataset/porto_nodes2segment.json', ) as nodes2seg_file:
    nodes2seg = nodes2seg_file.readline()
nodes2seg_json = json.loads(nodes2seg)

count = 0
for key in nodes2seg_json:
    for s_key in nodes2seg_json[key]:
        count += 1
print(count)