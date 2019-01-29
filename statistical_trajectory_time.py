import json
import time
import os


def time_str2time_stamp(s):
    time_array = time.strptime(s, "%Y-%m-%d %H:%M:%S")
    time_stamp = int(time.mktime(time_array))
    return time_stamp


def main(road_segment_file):

    trajectory_total_num = 0
    point_total_num = 0
    time_total_num = 0
    with open(road_segment_file, 'r') as f1:
        for line in f1:
            if line.startswith('"TRIP_ID"'):
                continue
            items = line.strip().split(',', 1)
            tid = items[0]
            if os.path.exists('tokyo/trajectory/tk_tra_new_transport_2_' + tid):
                print('tokyo/trajectory/tk_tra_new_transport_2_' + tid + 'exists!')
                tra_points = json.loads(items[1])
                points_size = len(tra_points)
                point_total_num += points_size
                trajectory_total_num += 1
                time_total_num += time_str2time_stamp(tra_points[-1]['time'].split('+')[0]) - time_str2time_stamp(tra_points[0]['time'].split('+')[0])


    # with open(road_segment_file2, 'r') as f2:
    #     for line in f2:
    #         if line.startswith('"TRIP_ID"'):
    #             continue
    #         items = line.strip().split(',', 1)
    #         tra_points = json.loads(items[1])
    #         points_size = len(tra_points)
    #         point_total_num += points_size
    #         trajectory_total_num += 1
    #         time_total_num += time_str2time_stamp(tra_points[-1]['time'].split('+')[0]) - time_str2time_stamp(tra_points[0]['time'].split('+')[0])

    print('trajectory_total_num: ', trajectory_total_num)
    print('point_total_num: ', point_total_num)
    print('time_total_num: ', time_total_num)
    print('average trajectory point: ', point_total_num/trajectory_total_num)
    print('average point time: ', time_total_num/point_total_num)


if __name__ == '__main__':

    main('tokyo/request/transport_2.request')
