import json
import time
import os


def time_str2time_stamp(s):
    time_array = time.strptime(s, "%Y-%m-%d %H:%M:%S")
    time_stamp = int(time.mktime(time_array))
    return time_stamp


def main(road_segment_file1, road_segment_file2):

    trajectory_total_num = 0
    point_total_num = 0
    time_total_num = 0
    with open(road_segment_file1, 'r') as f1:
        for line in f1:
            if line.startswith('"TRIP_ID"'):
                continue
            tra_points = json.loads(line)
            points_size = len(tra_points)
            point_total_num += points_size
            trajectory_total_num += 1
            time_total_num += tra_points[-1]['time'] - tra_points[0]['time']

    with open(road_segment_file2, 'r') as f2:
        for line in f2:
            if line.startswith('"TRIP_ID"'):
                continue
            tra_points = json.loads(line)
            points_size = len(tra_points)
            point_total_num += points_size
            trajectory_total_num += 1
            time_total_num += tra_points[-1]['time'] - tra_points[0]['time']

    print('trajectory_total_num: ', trajectory_total_num)
    print('point_total_num: ', point_total_num)
    print('time_total_num: ', time_total_num)
    print('average trajectory point: ', point_total_num/trajectory_total_num)
    print('average point time: ', time_total_num/point_total_num)


if __name__ == '__main__':

    main('sanfrancisco/sequence/sf_trajectory_road_segment_split350.sequence',
         'tokyo/sequence/tk_trajectory_transport_4_road_segment_split450.sequence')
