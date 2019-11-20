import json


def main(input_sequence, output):

    with open(output, 'w+') as f:
        for _node_seq in get_node_seqs(input_sequence):
            node_seq = []
            node_seq.append(_node_seq[0])
            pointer = 1
            for node in _node_seq[1:]:
                if _node_seq[pointer - 1] != node:
                    node_seq.append(node)
                pointer += 1
            f.write('%s\n' % ' 0 '.join(map(str, node_seq)))


def get_node_seqs(input_seq):
    with open(input_seq) as input_seq_file:
        for seq_line in input_seq_file:
            road_segment_seq = json.loads(seq_line)
            if len(road_segment_seq) < 10:
                continue
            else:
                _node_seq = segments2node_seq(road_segment_seq)
                yield _node_seq


def segments2node_seq(road_segment_seq):
    nodes = []
    is_head = True
    for road_segment in road_segment_seq:
        if is_head:
            node = road_segment["target"] if road_segment["heading"] == "forward" else road_segment["source"]
            nodes.append(node)
            is_head = False
        else:
            if road_segment["heading"] == "forward":
                _source = road_segment["source"]
                _target = road_segment["target"]
            else:
                _source = road_segment["target"]
                _target = road_segment["source"]
            nodes.append(_source)
            nodes.append(_target)
    return nodes


if __name__ == '__main__':

    main(input_sequence='porto/sequence/pt_trajectory_road_segment.sequence',
         output='porto/sequence/pt_trajectory_sequence.node')