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


def get_segments_from_roads(sequence):
    _segments = []
    for road in sequence:
        road_id = road['road']
        _segments.append(road_id)
    return _segments


def main(input_file, output_travel_time):

    output_node = open(output_travel_time, 'w+')

    with open(input_file, 'r') as f:
        for line in f:
            segments_sequence = json.loads(line)
            if len(segments_sequence) < 15:
                continue
            travel_time = segments_sequence[-1]['time'] - segments_sequence[0]['time']
            node_sequence = get_segments_from_roads(segments_sequence)
            output_node.write('%s\n' % ' '.join(map(str, node_sequence + [travel_time])))

    output_node.close()


main(input_file='porto/sequence/pt_trajectory_road_segment.sequence',
     output_travel_time='porto/sequence/pt_trajectory_node_travel_time.travel')
