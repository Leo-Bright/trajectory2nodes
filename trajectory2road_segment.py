import psycopg2
import json
import os


def get_inputs(directory):
    all_files = os.listdir(directory)
    for file in all_files:
        if file.find('_new_') >= 0:
            with open(directory + file, 'r') as f:
                yield json.loads(f.readline())


def equal_the_last(sequence, obj):
    if len(sequence) == 0:
        return False
    else:
        tmp = sequence[-1]
        if type(tmp) == int:
            return tmp == obj
        return road_compare(tmp, obj)


def road_compare(a, b):
    if a['road'] == b['road'] and a['heading'] == b['heading']:
        return True
    return False


def trajectories2road_sequence(input_dir):
    input_trajectories = get_inputs(input_dir)
    for trajectory in input_trajectories:
        road_sequence = []
        # trajectory_size = len(trajectory)
        for index, matched in enumerate(trajectory):
            if 'transition' not in matched:
                road = matched['point']
                road['time'] = matched['time']
                if not equal_the_last(road_sequence, road):
                    road_sequence.append(road)

            else:
                roads = matched['transition']['route']['roads']
                if len(roads) > 1:
                    cur_time = matched['time']
                    last_matched = trajectory[index - 1]
                    last_time = last_matched['time']
                    time_seg = (cur_time - last_time) // (len(roads) - 1)
                    for i, road in enumerate(roads[1:]):
                        if not equal_the_last(road_sequence, road):
                            new_time = time_seg * (i + 1) + last_time
                            road['time'] = new_time
                            road_sequence.append(road)
                else:
                    continue

        yield road_sequence


def main(input_dir, output_road, output_node):

    road2node = {}
    conn = psycopg2.connect(database="tokyo", user="osmuser", password="pass", host="172.19.7.241", port="5432")
    cur = conn.cursor()
    sql = 'select gid, source, target from bfmap_ways;'
    cur.execute(sql)
    rows = cur.fetchall()

    for row in rows:
        (gid, source, target) = row
        road2node[gid] = (source, target)

    road_file = open(output_road, 'w+')
    node_file = open(output_node, 'w+')

    for road_sequence in trajectories2road_sequence(input_dir):
        node_sequence = []
        new_road_sequence = []

        for road in road_sequence:
            gid = road['road']
            (source, target) = road2node[gid]
            road['source'] = source
            road['target'] = target
            if road['heading'] == 'forward':
                if not equal_the_last(node_sequence, source):
                    node_sequence.append(source)
                node_sequence.append(target)
            else:
                if not equal_the_last(node_sequence, target):
                    node_sequence.append(target)
                node_sequence.append(source)
            new_road_sequence.append(road)

        if len(new_road_sequence) >= 10:
            road_file.write(json.dumps(new_road_sequence) + '\n')
        nodes_size = len(node_sequence)

        patch_size = 1280

        nodes_patch_num = nodes_size // patch_size
        for index in range(nodes_patch_num + 1):
            start = index * patch_size
            if index == nodes_patch_num:
                end = len(node_sequence)
            else:
                end = (index + 1) * patch_size

            node_patch = node_sequence[start:end]
            if len(node_patch) > 10:
                node_file.write('%s\n' % ' 0 '.join(map(str, node_patch)))

    road_file.close()
    node_file.close()


main(input_dir='tokyo/trajectory/',
     output_road='tokyo/sequence/tk_trajectory_road_segment.sequence',
     output_node='tokyo/sequence/tk_trajectory_node.sequence')
