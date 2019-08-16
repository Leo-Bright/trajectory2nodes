import json


def extract_segment_key(road_segments_file, way_tag_file, key, output_file):
    segments_key = {}
    way_tag = {}
    with open(way_tag_file) as f:
        way_tag = json.loads(f.readline())

    with open(road_segments_file) as f:
        road_segments = json.loads(f.readline())
        for rid in road_segments:
            item = road_segments[rid]
            wid = item['osm_id']
            if key not in way_tag[str(wid)]:
                continue
            value = way_tag[str(wid)][key]
            segments_key[rid] = value

    with open(output_file, 'w+') as f:
        f.write(json.dumps(segments_key))


if __name__ == '__main__':
    extract_segment_key(road_segments_file='sanfrancisco/dataset/all_road_segments_dict.sanfrancisco',
                           way_tag_file='sanfrancisco/dataset/sf_road_highway_tag.json',
                           key='tiger:name_base',
                           output_file='sanfrancisco/dataset/sf_segments_tiger_namebase_type.json')
