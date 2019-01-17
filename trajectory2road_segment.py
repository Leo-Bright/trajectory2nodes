import psycopg2
import json
import os


def get_inputs(directory):
    all_files = os.listdir(directory)
    for file in all_files:
        if file.find('.trajectory_new') >= 0:
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
        trajectory_size = len(trajectory)
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
                    if index >= trajectory_size - 1:
                        for road in roads[1:]:
                            if not equal_the_last(road_sequence, road):
                                road['time'] = cur_time
                                road_sequence.append(road)
                    else:
                        next_matched = trajectory[index + 1]
                        next_time = next_matched['time']
                        time_seg = (next_time - cur_time) // (len(roads) - 1)
                        for i, road in enumerate(roads[1:]):
                            if not equal_the_last(road_sequence, road):
                                new_time = time_seg * (i + 1) + cur_time
                                road['time'] = new_time
                                road_sequence.append(road)
                else:
                    continue
        for i in range(len(road_sequence) - 1):
            if i == 0:
                continue

        yield road_sequence


def main(input_dir, output_road, output_node):

    road2node = {}
    conn = psycopg2.connect(database="sanfrancisco", user="osmuser", password="pass", host="172.19.7.241", port="5432")
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

        road_file.write(json.dumps(new_road_sequence) + '\n')
        node_file.write('%s\n' % ' 0 '.join(map(str, node_sequence)))

    road_file.close()
    node_file.close()


main(input_dir='sanfrancisco/trajectory/',
     output_road='sanfrancisco/sequence/sf_trajectory_road_segment.sequence',
     output_node='sanfrancisco/sequence/sf_trajectory_node.sequence')
