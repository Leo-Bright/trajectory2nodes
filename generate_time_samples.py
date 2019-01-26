import json


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


def get_nodes_from_roads(road_sequence):
    node_sequence = []
    for road in road_sequence:
        start = road['source'] if road['heading'] == 'forward' else road['target']
        end = road['target'] if road['heading'] == 'forward' else road['source']
        if not equal_the_last(node_sequence, start):
            node_sequence.append(start)
        node_sequence.append(end)
    return node_sequence


def main(input_file, output_file, intervals):

    output = open(output_file, 'w+')

    with open(input_file, 'r') as f:
        for line in f:
            road_sequence = json.loads(line)
            size = len(road_sequence)
            start = 0
            flag = False
            for i in range(size):
                travel_time = road_sequence[i]['time'] - road_sequence[start]['time']
                if travel_time > intervals:
                    if flag:
                        node_sequence = get_nodes_from_roads(last_road_sequence)
                        output.write('%s\n' % ' '.join(map(str, node_sequence + [last_travel_time])))
                    last_road_sequence = road_sequence[start:i + 1]
                    last_travel_time = travel_time
                    start = i + 1
                    flag = True
                # print('start_road:', start_road, '\t end_node:', end_road)
                elif i == size - 1:
                    if not flag:
                        continue
                    last_road_sequence += road_sequence[start:]
                    node_sequence = get_nodes_from_roads(last_road_sequence)
                    output.write('%s\n' % ' '.join(map(str, node_sequence + [travel_time])))
                else:
                    continue

    output.close()


main(input_file='tokyo/sequence/tk_trajectory_transport_2_road_segment.sequence',
     output_file='tokyo/sequence/tk_transport_2_travel_time_45.samples',
     intervals=45)
