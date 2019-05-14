import json


def run(road_segments_file, output_segment_type_file):
    with open(road_segments_file) as f:
        road_segments = json.loads(f.readline())

    with open(output_segment_type_file, 'w+') as f:
        for id in road_segments:
            segment = road_segments[id]
            class_id = segment['class_id']
            f.write(id + ' ' + str(class_id) + '\n')


if __name__ == '__main__':
    run(road_segments_file='sanfrancisco/dataset/all_road_segments_dict.sanfrancisco',
        output_segment_type_file='sanfrancisco/dataset/segments_type.sanfrancisco',
        )