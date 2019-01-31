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


def main(input_file, output_road_segment, output_travel_time, intervals):

    output_road = open(output_road_segment, 'w+')
    output_node = open(output_travel_time, 'w+')

    with open(input_file, 'r') as f:
        for line in f:
            road_sequence = json.loads(line)
            if not intervals:
                travel_time = road_sequence[-1]['time'] - road_sequence[0]['time']
                node_sequence = get_nodes_from_roads(road_sequence)
                output_node.write('%s\n' % ' '.join(map(str, node_sequence + [travel_time])))
            else:
                size = len(road_sequence)
                start = 0
                have_last = False
                for i in range(size):
                    travel_time = road_sequence[i]['time'] - road_sequence[start]['time']
                    if travel_time > intervals:
                        if have_last:
                            node_sequence = get_nodes_from_roads(last_road_sequence)
                            output_node.write('%s\n' % ' '.join(map(str, node_sequence + [last_travel_time])))
                            output_road.write(json.dumps(last_road_sequence) + '\n')
                        last_road_sequence = road_sequence[start:i + 1]
                        last_travel_time = travel_time
                        start = i + 1
                        have_last = True
                    # print('start_road:', start_road, '\t end_node:', end_road)
                    elif i == size - 1:
                        if not have_last:
                            cur_road_sequence = road_sequence[start:]
                        else:
                            cur_road_sequence = last_road_sequence + road_sequence[start:]
                        node_sequence = get_nodes_from_roads(cur_road_sequence)
                        output_node.write('%s\n' % ' '.join(map(str, node_sequence + [travel_time])))
                        output_road.write(json.dumps(cur_road_sequence) + '\n')
                    else:
                        continue

    output_road.close()
    output_node.close()


main(input_file='porto/sequence/pt_trajectory_road_segment.sequence',
     output_road_segment='porto/sequence/pt_trajectory_road_segment_split450.sequence',
     output_travel_time='porto/sequence/pt_trajectory_node_travel_time_450.travel',
     intervals=450)
