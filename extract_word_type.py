import json


def extract_word_type(road_segments_file, network_file, word_type_file):
    all_nodes = set()
    with open(road_segments_file) as f:
        road_segments = json.loads(f.readline())

    with open(network_file) as f:
        for line in f:
            for node in line.strip().split(' '):
                all_nodes.add(node)

    with open(word_type_file, 'w+') as f:
        for key in road_segments:
            f.write(key + ' 1\n')

        for node in all_nodes:
            f.write(node + ' 0\n')


if __name__ == '__main__':
    extract_word_type(road_segments_file='porto/dataset/all_road_segments_dict_old.porto',
                      network_file='porto/dataset/porto_old.network',
                      word_type_file='porto/dataset/pt_word_old.type')
