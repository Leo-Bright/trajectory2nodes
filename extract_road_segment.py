import psycopg2
import json

city = 'newyork'

# 数据库连接参数
conn = psycopg2.connect(database=city, user="osmuser", password="pass", host="172.19.7.242", port="5432")
cur = conn.cursor()

cur.execute("select gid, osm_id, class_id, source, target, reverse, priority from bfmap_ways;")
rows = cur.fetchall()        # all rows in table
print("segments count:", len(rows))
way2nodes = {}  # {osmid_way: [osmid_node,...]}
road_segments = []
all_road_segment_dict = {}
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
    all_road_segment_dict[gid] = {"osm_id": osm_id, "source": source, "target": target, "reverse": reverse, "class_id": class_id}
conn.commit()
cur.close()
conn.close()
print(len(nodes_count.keys()))

with open(r'all_road_segments_dict.' + city, 'w+') as f:
    f.write(json.dumps(all_road_segment_dict))

with open(r'node_types.' + city, 'w+') as node_type:
    for key in node_type2node_id:
        for node in node_type2node_id[key]:
            node_type.write(str(node) + ' ' + str(key) + '\n')

with open(r'segments_type.' + city, 'w+') as segment_type:
    for key in segment_type2segment_id:
        for segment in segment_type2segment_id[key]:
            segment_type.write(str(segment) + ' ' + str(key) + '\n')

with open(city + r'_road_segment.network', 'w+') as network:
    network.write("segment_id\tosm_way_id\tclass_id\tsource_node_id\ttarget_node_id\tpriority\n")
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

with open(city + r'_LINE.network', 'w+') as network:
    network.write("source_node_id\ttarget_node_id\tpriority\n")
    for road_segment in road_segments:
        gid, osm_id, class_id, source, target, reverse, priority = road_segment
        if source == target:
            continue
        if float(reverse) >= 0.0:
            network.write(str(source) + " " + str(target) + " " + str(priority))
            network.write("\n")
            network.write(str(target) + " " + str(source) + " " + str(priority))
            network.write("\n")
        else:
            network.write(str(source) + " " + str(target) + " " + str(priority))
            network.write("\n")

with open(city + r'.network', 'w+') as network:
    network.write("source_node_id\ttarget_node_id\n")
    for road_segment in road_segments:
        gid, osm_id, class_id, source, target, reverse, priority = road_segment
        if source == target:
            continue
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

with open(city + r'_nodes2segment.json', 'w+') as nodes2segment_file:
    nodes2segment_file.write(json.dumps(nodes2segment))


