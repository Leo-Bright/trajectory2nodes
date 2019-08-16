import json


def extract_segment_class_id(road_segments_file, class_id, output_file):
    segments_class_id = {}
    with open(road_segments_file) as f:
        road_segments = json.loads(f.readline())
        for key in road_segments:
            item = road_segments[key]
            if item['class_id'] != class_id:
                continue
            segments_class_id[key] = class_id

    with open(output_file, 'w+') as f:
        f.write(json.dumps(segments_class_id))


if __name__ == '__main__':
    extract_segment_class_id(road_segments_file='porto/dataset/all_road_segments_dict.porto',
                           class_id=106,
                           output_file='porto/dataset/pt_segments_class_id_106.json')
