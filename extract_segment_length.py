import json
from math import radians, cos, sin, asin, sqrt


def extract_segment_lenght(road_segments_path, coords_path, output_path, network_path, length_path):

    with open(coords_path) as coords_file:
        node2coords = json.loads(coords_file.readline())

    segments_length = {}

    with open(road_segments_path) as f:
        road_segments = json.loads(f.readline())
        for key in road_segments:
            items = road_segments[key]
            source = str(items['source'])
            target = str(items['target'])
            if source == target:
                continue
            source_lon_lat = node2coords[source]
            target_lon_lat = node2coords[target]

            segment_distance = get_distance(source_lon_lat[0], source_lon_lat[1], target_lon_lat[0], target_lon_lat[1])

            segments_length[key] = (source, target, segment_distance)

    with open(output_path, 'w+') as f:
        f.write(json.dumps(segments_length))

    with open(network_path, 'w+') as f:
        for key in segments_length:
            items = segments_length[key]
            f.write(items[0] + ' ' + items[1] + ' ' + str(items[2]) + '\n')

    with open(length_path, 'w+') as f:
        for key in segments_length:
            items = segments_length[key]
            f.write(key + ' ' + str(items[2]) + '\n')


def get_distance(lng1, lat1, lng2, lat2):
    lng1, lat1, lng2, lat2 = map(radians, [lng1, lat1, lng2, lat2])
    dlon = lng2-lng1
    dlat = lat2-lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    dis = 2*asin(sqrt(a))*6371*1000
    return dis


if __name__ == '__main__':
    extract_segment_lenght(road_segments_path='newyork/dataset/all_road_segments_dict.newyork',
                           coords_path='newyork/dataset/node2coords.json',
                           output_path='newyork/dataset/newyork_road_segments_length.json',
                           network_path='newyork/network/newyork_distance.network',
                           length_path='newyork/dataset/newyork_road_segments.length')
