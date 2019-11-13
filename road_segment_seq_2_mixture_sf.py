import json


def main(input_sequence, output_mixture):

    with open(output_mixture, 'w+') as f:
        for _mixture in get_mixtures(input_sequence):
            mixture = []
            mixture.append(_mixture[0])
            pointer = 1
            for word in _mixture[1:]:
                if _mixture[pointer - 1] != word:
                    mixture.append(word)
                pointer += 1
            f.write('%s\n' % ' 0 '.join(map(str, mixture)))


def get_mixtures(input_seq):
    with open(input_seq) as input_seq_file:
        for seq_line in input_seq_file:
            road_segment_seq = json.loads(seq_line)
            if len(road_segment_seq) < 10:
                continue
            else:
                _mixture = segments2mixture(road_segment_seq)
                yield _mixture


def segments2mixture(road_segment_seq):
    words = []
    is_head = True
    for road_segment in road_segment_seq:
        if is_head:
            words.append(road_segment["road"])
            node = road_segment["target"] if road_segment["heading"] == "forward" else road_segment["source"]
            words.append(node)
            is_head = False
        else:
            if road_segment["heading"] == "forward":
                _source = road_segment["source"]
                _target = road_segment["target"]
            else:
                _source = road_segment["target"]
                _target = road_segment["source"]
            words.append(_source)
            words.append(road_segment["road"])
            words.append(_target)
    return words


if __name__ == '__main__':

    main(input_sequence='sanfrancisco/sequence/sf_trajectory_road_segment.sequence',
         output_mixture='sanfrancisco/sequence/sf_trajectory_road_segment.mixture')