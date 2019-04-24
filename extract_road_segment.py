import psycopg2
from gensim.models import Word2Vec
import json

# 数据库连接参数
conn = psycopg2.connect(database="porto", user="osmuser", password="pass", host="localhost", port="5432")
cur = conn.cursor()

cur.execute("select gid, osm_id, class_id, source, target, reverse, priority from bfmap_ways;")
rows = cur.fetchall()        # all rows in table
print("segments count:", len(rows))
way2nodes = {}  # {osmid_way: [osmid_node,...]}
road_segments = []
node_type2node_id = {}
segment_type2segment_id = {}
nodes_count = {}
for row in rows:
    gid, osm_id, class_id, source, target, reverse, priority = row
    if class_id not in node_type2node_id:
        node_type2node_id[class_id] = {}
    node_type2node_id[class_id][source] = True
    node_type2node_id[class_id][target] = True
    if class_id not in segment_type2segment_id:
        segment_type2segment_id[class_id] = []
    segment_type2segment_id[class_id].append(gid)
    if source not in nodes_count:
        nodes_count[source] = 1
    else:
        nodes_count[source] += 1
    if target not in nodes_count:
        nodes_count[target] = 1
    else:
        nodes_count[target] += 1
    road_segments.append((gid, osm_id, class_id, source, target, reverse, priority))
conn.commit()
cur.close()
conn.close()
print(len(nodes_count.keys()))

with open(r'node_types.porto', 'w+') as node_type:
    for key in node_type2node_id:
        for node in node_type2node_id[key]:
            node_type.write(str(node) + ' ' + str(key) + '\n')

with open(r'segment_types.porto', 'w+') as segment_type:
    for key in segment_type2segment_id:
        for segment in segment_type2segment_id[key]:
            segment_type.write(str(segment) + ' ' + str(key) + '\n')

with open(r'porto_road_segment.network', 'w+') as network:
    network.write("source_node_id\ttarget_node_id\tpriority\n")
    for road_segment in road_segments:
        gid, osm_id, class_id, source, target, reverse, priority = road_segment
        if float(reverse) >= 0.0:
            network.write(str(gid) + " " + str(osm_id) + " " + str(class_id) + " " + str(source) + " " + str(target) + " " + str(priority))
            network.write("\n")
            network.write(str(gid) + " " + str(osm_id) + " " + str(class_id) + " " + str(target) + " " + str(source) + " " + str(priority))
            network.write("\n")
        else:
            network.write(str(gid) + " " + str(osm_id) + " " + str(class_id) + " " + str(source) + " " + str(target) + " " + str(priority))
            network.write("\n")

with open(r'porto_LINE.network', 'w+') as network:
    network.write("source_node_id\ttarget_node_id\tpriority\n")
    for road_segment in road_segments:
        gid, osm_id, class_id, source, target, reverse, priority = road_segment
        if float(reverse) >= 0.0:
            network.write(str(source) + " " + str(target) + " " + str(priority))
            network.write("\n")
            network.write(str(target) + " " + str(source) + " " + str(priority))
            network.write("\n")
        else:
            network.write(str(source) + " " + str(target) + " " + str(priority))
            network.write("\n")

with open(r'porto.network', 'w+') as network:
    network.write("source_node_id\ttarget_node_id\n")
    for road_segment in road_segments:
        gid, osm_id, class_id, source, target, reverse, priority = road_segment
        network.write(str(source) + " " + str(target))
        network.write("\n")

nodes2segment = {}
for road_segment in road_segments:
    gid, osm_id, class_id, source, target, reverse, priority = road_segment
    if source not in nodes2segment:
        nodes2segment[source] = {}
    nodes2segment[source][target] = gid
    if target not in nodes2segment:
        nodes2segment[target] = {}
    nodes2segment[target][source] = gid

with open(r'porto_nodes2segment.json', 'w+') as nodes2segment_file:
    nodes2segment_file.write(json.dumps(nodes2segment))



# walks = []
# f_500K_ways = open(r'500k.w2v.txt', 'r')
# f_500K_node2vec = open(r'500k.nodes.vector', 'w+')
# f_500K_nodes = open(r'500k.nodes.txt', 'r')
# for line in f_500K_ways.readlines():
#     output_nodes = []
#     way_list = line.strip().split(" ")
#     if len(way_list) < 1: continue
#     for way in way_list:
#         way = way.strip()
#         if len(way) < 1 : continue
#         osmid_way, heading = way.split("-")
#         if osmid_way not in way2nodes: continue
#         nodes = way2nodes[osmid_way]
#         if heading == "forward":
#             for node in nodes:
#                 output_nodes.append(str(node))
#         else:
#             index = len(nodes) - 1
#             while index >= 0:
#                 node = nodes[index]
#                 output_nodes.append(node)
#                 index -= 1
#     # f_500K_nodes.write(str(output_nodes) + '\n')
#     if len(output_nodes) > 1:
#         walks.append(output_nodes)
#
# f_500K_nodes.write(json.dumps(walks))
#
# data = json.loads(f_500K_nodes.readline())
# print(len(data))
# count = 0
# for tra in data:
#     print(count)
#     count += 1
#     if len(tra) < 10:
#         continue
#     trajectory = []
#     for item in tra:
#         if type(item)==int:
#             item = str(item)
#         trajectory.append(item)
#     walks.append(trajectory)
#
# model = Word2Vec(walks, size=64, window=5, min_count=0, sg=1, hs=1, workers=4)
# model.save('mymodel')
# model.wv.save_word2vec_format("500k.nodes.vector")

# f_500K_ways.close()
# f_500K_node2vec.close()
# f_500K_nodes.close()
