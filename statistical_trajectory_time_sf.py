import json
import time
import os
import pandas as pd


def time_str2time_stamp(s):
    time_array = time.strptime(s, "%Y-%m-%d %H:%M:%S")
    time_stamp = int(time.mktime(time_array))
    return time_stamp


def get_gps_point_trajectories(input_dir, regex):
    gps_point_trajectories = []
    file_names = os.listdir(input_dir)
    for file_name in file_names:
        if file_name.find(regex) >= 0:
            gps_point_trajectories.append(file_name)

    total_point = 0
    total_time = 0
    print('file number:', len(gps_point_trajectories))
    for gps_point_trajectory in gps_point_trajectories:
        data_matrix = pd.read_csv(input_dir + gps_point_trajectory, header=None, sep=' ', index_col=None)
        latitude = data_matrix[0]
        longitude = data_matrix[1]
        occupancy = data_matrix[2]
        time_gps = data_matrix[3]

        new_data_matrix = pd.DataFrame()
        new_data_matrix['longitude'] = longitude
        new_data_matrix['latitude'] = latitude
        new_data_matrix['occupancy'] = occupancy
        new_data_matrix['time'] = time_gps

        trajectory = []

        time_first = time_gps.iloc[0]
        time_last = time_gps.iloc[-1]
        time_gap = time_first - time_last
        total_time += time_gap
        total_point += time_gps.size

    print('total_point: ', total_point)
    print('total_time: ', total_time)
    print('average_time_per_point: ', total_time/total_point)


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
    get_gps_point_trajectories(input_dir='sanfrancisco/dataset/',
                                regex='.txt',)
