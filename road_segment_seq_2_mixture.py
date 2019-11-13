import json


def main(input_seq, segment_dict, output_seq):
    with open(segment_dict) as segment_dict_file:
        segment_dict_line = segment_dict_file.readline()
        segment2node = json.loads(segment_dict_line)
    with open(output_seq, 'w+') as f:
        for real_nodes, segments in get_sequences(input_seq, segment2node):
            _mixtures = []
            assert len(real_nodes)-1 == len(segments)
            for idx in range(len(segments)):
                _mixtures.append(real_nodes[idx])
                _mixtures.append(segments[idx])
            _mixtures.append(real_nodes[-1])
            f.write('%s\n' % ' 0 '.join(map(str, _mixtures)))


def get_sequences(input_seq, segment2node):
    with open(input_seq) as input_seq_file:
        for seq_line in input_seq_file:
            road_segment_seq = json.loads(seq_line)
            if len(road_segment_seq) < 10:
                continue
            else:
                real_nodes, segments = trans_node_to_segment(nodes, node2segment)
                yield real_nodes, segments


def trans_node_to_segment(nodes, node2segment):
    segments = []
    real_nodes = []
    last_node = None
    for node in nodes[::2]:
        real_nodes.append(node)
        if last_node is not None:
            _segment = node2segment[last_node]
            segment = _segment[node]
            segments.append(segment)
        last_node = node
    return real_nodes, segments


if __name__ == '__main__':

    main(input_seq='sanfrancisco/sequence/sf_trajectory_road_segment.sequence',
         segment_dict='sanfrancisco/dataset/all_road_segments_dict.sanfrancisco',
         output_seq='sanfrancisco/sequence/sf_trajectory_road_segment.mixture')