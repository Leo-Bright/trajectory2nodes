import json


def main(input_sequence, output):

    with open(output, 'w+') as f:
        for _seq in get_seqs(input_sequence):
            seq = []
            seq.append(_seq[0])
            pointer = 1
            for ele in _seq[1:]:
                if _seq[pointer - 1] != ele:
                    seq.append(ele)
                pointer += 1
            f.write('%s\n' % ' 0 '.join(map(str, seq)))


def get_seqs(input_seq):
    with open(input_seq) as input_seq_file:
        for seq_line in input_seq_file:
            road_segment_seq = json.loads(seq_line)
            if len(road_segment_seq) < 10:
                continue
            else:
                _seq = trajectory2node_seq(road_segment_seq)
                yield _seq


def trajectory2node_seq(road_segment_seq):
    eles = []
    for road_segment in road_segment_seq:
        segment = road_segment["road"]
        eles.append(segment)
    return eles


if __name__ == '__main__':

    main(input_sequence='porto/sequence/pt_trajectory_road_segment.sequence',
         output='porto/sequence/pt_trajectory_sequence.segment')