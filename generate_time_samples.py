import json


def main(input_file, output_file, intervals):

    output = open(output_file, 'w+')

    with open(input_file, 'r') as f:
        for line in f:
            road_sequence = json.loads(line)
            size = len(road_sequence)
            for inter in intervals:
                inter_size = size // inter
                for i in range(inter_size):
                    start_road = road_sequence[i * inter]
                    end_road = road_sequence[-1] if i == inter_size -1 else road_sequence[(i + 1) * inter]
                    start_node = start_road['source'] if start_road['heading'] == 'forward' else start_road['target']
                    end_node = end_road['source'] if end_road['heading'] == 'backwark' else end_road['target']
                    travel_time = end_road['time'] - start_road['time']
                    # print('start_road:', start_road, '\t end_node:', end_road)
                    # print('start_time:', start_road['time'], '\t end_time:', end_road['time'], '\t travel_time:', travel_time)
                    output.write('%s\n' % ' '.join(map(str, [start_node, end_node, travel_time])))
    output.close()


main(input_file='sanfrancisco/sequence/sf_trajectory_road_segment.sequence',
     output_file='sanfrancisco/sequence/sf_travel_time_21.samples',
     intervals=(21, ))
