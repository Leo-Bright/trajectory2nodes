import json
import time


def time_str2time_stamp(s):
    time_array = time.strptime(s, "%Y-%m-%d %H:%M:%S")
    time_stamp = int(time.mktime(time_array))
    return time_stamp


def main(road_segment_file):

    trajectory_total_num = 0
    segs_total_num = 0
    time_total_num = 0
    with open(road_segment_file, 'r') as f:
        for line in f:
            if line.startswith('"TRIP_ID"'):
                continue
            # items = line.strip().split(',', 1)
            road_segments = json.loads(line)
            seg_size = len(road_segments)
            segs_total_num += seg_size
            trajectory_total_num += 1
            # time_total_num += time_str2time_stamp(tra_points[-1]['time'].split('+')[0]) - time_str2time_stamp(tra_points[0]['time'].split('+')[0])
            time_total_num += road_segments[-1]['time'] - road_segments[0]['time']

    print('trajectory_total_num: ', trajectory_total_num)
    print('segs_total_num: ', segs_total_num)
    print('time_total_num: ', time_total_num)
    print('average trajectory segs: ', segs_total_num/trajectory_total_num)
    print('average segment times: ', time_total_num/segs_total_num)


if __name__ == '__main__':

    main('tokyo/sequence/tk_trajectory_transport_4_road_segment.sequence', )
