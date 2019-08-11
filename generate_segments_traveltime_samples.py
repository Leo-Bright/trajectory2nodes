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


def main(input_file, output_travel_time, intervals):

    output_node = open(output_travel_time, 'w+')

    with open(input_file, 'r') as f:
        for line in f:
            segments_sequence = json.loads(line)
            if len(segments_sequence) < 15:
                continue
            if not intervals:
                travel_time = segments_sequence[-1]['time'] - segments_sequence[0]['time']
                segments = get_segments_from_roads(segments_sequence)
                output_node.write('%s\n' % ' '.join(map(str, segments + [travel_time])))
            else:
                size = len(segments_sequence)
                start = 0
                have_last = False
                for i in range(size):
                    travel_time = segments_sequence[i]['time'] - segments_sequence[start]['time']
                    if travel_time > intervals:
                        if have_last:
                            segments = get_segments_from_roads(last_road_sequence)
                            output_node.write('%s\n' % ' '.join(map(str, segments + [last_travel_time])))
                        last_road_sequence = segments_sequence[start:i + 1]
                        last_travel_time = travel_time
                        start = i + 1
                        have_last = True
                    # print('start_road:', start_road, '\t end_node:', end_road)
                    elif i == size - 1:
                        if not have_last:
                            cur_road_sequence = segments_sequence[start:]
                        else:
                            cur_road_sequence = last_road_sequence
                        segments = get_segments_from_roads(cur_road_sequence)
                        output_node.write('%s\n' % ' '.join(map(str, segments + [travel_time])))
                    else:
                        continue

    output_node.close()


main(input_file='porto/sequence/pt_trajectory_road_segment.sequence',
     output_travel_time='porto/sequence/pt_trajectory_node_travel_time.travel',
     intervals=None)
