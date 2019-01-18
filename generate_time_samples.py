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
            for inter in intervals:
                inter_size = size // inter
                for i in range(inter_size):
                    split_road_sequence = road_sequence[i * inter:-1] if i == inter_size - 1 else road_sequence[i * inter:(i + 1) * inter]
                    node_sequence = get_nodes_from_roads(split_road_sequence)
                    travel_time = split_road_sequence[0]['time'] - split_road_sequence[-1]['time']
                    # print('start_road:', start_road, '\t end_node:', end_road)
                    output.write('%s\n' % ' '.join(map(str, node_sequence + [travel_time])))
    output.close()


main(input_file='sanfrancisco/sequence/sf_trajectory_road_segment.sequence',
     output_file='sanfrancisco/sequence/sf_travel_time_21.samples',
     intervals=(21, ))
