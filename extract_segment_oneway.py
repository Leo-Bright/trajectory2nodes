import json


def extract_segment_oneway(road_segments_file, output_file):
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


if __name__ == '__main__':
    extract_segment_oneway(road_segments_file='sanfrancisco/dataset/all_road_segments_dict.sanfrancisco',
                           output_file='sanfrancisco/dataset/road_segments_oneway.json')
