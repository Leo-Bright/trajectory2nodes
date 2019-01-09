import pandas as pd
import time
import json
import os
import sys
import optparse
import socket
import random

host = 'localhost'
port = '1234'
format = 'slimjson'



def main(input_dir, regex, output):

    matcher_result = []

    trajectories = get_trajectories(input_dir, regex)
    for trajectory in trajectories:
        part_count = len(trajectory) // 500
        for index in range(part_count):
            if index == part_count - 1:
                start = 0
            else:
                start = len(trajectory) - (index + 1) * 500
            end = len(trajectory) - index * 500
            samples = trajectory[start:end]
            tmp = "batch-%s" % random.randint(0, sys.maxsize)
            file = open(tmp, "w")
            try:
                try:
                    file.write(
                        "{\"format\": \"%s\", \"request\": %s}\n" %
                        (format, json.dumps(samples))
                    )
                finally:
                    file.close()

                output = ''

                s = socket.create_connection((host, port))
                try:
                    with open(tmp, 'rb') as f:
                        s.sendall(f.read())
                    s.shutdown(socket.SHUT_WR)
                    buf = s.recv(4096)
                    while buf:
                        if len(output) < 16:
                            output += buf.decode()
                        # sys.stdout.write(buf.decode())
                        print(buf.decode())
                        print(type(buf.decode()))
                        buf = s.recv(4096)
                finally:
                    s.close()
            finally:
                os.remove(tmp)

            if not output.startswith('SUCCESS\n'):
                sys.exit(1)



def get_trajectories(input_dir, regex):
    trajectory_files = []
    file_names = os.listdir(input_dir)
    for file_name in file_names:
        if file_name.find(regex) >= 0:
            trajectory_files.append(file_name)

    for trajectory_file in trajectory_files:
        data_matrix = pd.read_csv(input_dir + trajectory_file, header=None, sep=' ', index_col=None)
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

        for row in new_data_matrix.itertuples():
            # print(round(getattr(row, 'longitude'), 5), round(getattr(row, 'latitude'), 5))
            position = 'POINT(' + str(round(getattr(row, 'longitude'), 5)) + ' ' + str(round(getattr(row, 'latitude'), 5)) + ')'
            point = {
                "point": position,
                "time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(getattr(row, 'time')))) + '-0800',
                "id": "1"
            }
            trajectory.append(point)

        yield trajectory


main(input_dir='sanfrancisco/dataset/',
     regex='.txt',
     output='sanfrancisco/trajectory/sanfrancisco.trajectory')