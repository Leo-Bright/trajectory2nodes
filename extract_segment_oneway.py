import json


def extract_segment_oneway(road_segments_file, output_file, oneway_file):
    segments_oneway = {}
    with open(road_segments_file) as f:
        road_segments = json.loads(f.readline())
        for key in road_segments:
            item = road_segments[key]
            if item['reverse'] > 0:
                continue
            segments_oneway[key] = 'oneway'

    with open(output_file, 'w+') as f:
        f.write(json.dumps(segments_oneway))

    with open(oneway_file, 'w+') as f:
        for key in road_segments:
            item = road_segments[key]
            if item['reverse'] > 0:
                f.write(key + ' 1\n')
            else:
                f.write(key + ' 0\n')


if __name__ == '__main__':
    extract_segment_oneway(road_segments_file='porto/dataset/all_road_segments_dict_old.porto',
                           output_file='porto/dataset/pt_road_segments_oneway_old.json',
                           oneway_file='porto/dataset/pt_road_segments_old.oneway')
