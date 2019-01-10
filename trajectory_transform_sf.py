import pandas as pd
import time
import json
import os
import sys
import optparse
import socket
import random
from multiprocessing import Process, Pool


class Trajectory(object):

    host = ['172.19.7.241']
    port = '1234'
    format = 'slimjson'
    process_id = 0

    def main(self, input_dir, regex, output, threads):

        pool = Pool(threads)

        trajectories = self.get_trajectories(input_dir, regex)
        for trajectory in trajectories:
            host_idx = random.randint(0, len(self.host) - 1)
            pool.apply_async(func=self.process_trajectory,
                             args=(trajectory, host_idx),
                             callback=self.post_process_trajectory)
        pool.close()
        pool.join()

    def process_trajectory(self, trajectory, host_index):
        host = self.host[host_index]
        port = self.port
        part_count = len(trajectory) // 200
        for index in range(part_count + 1):
            if index == part_count:
                start = 0
            else:
                start = len(trajectory) - (index + 1) * 200
            end = len(trajectory) - index * 200
            samples = trajectory[start:end]
            # tmp = "batch-%s" % random.randint(0, sys.maxsize)
            # file = open(tmp, "w")
            post_str = '{' + '"format": {format}, "request": {samples}'.format(format=self.format, samples=json.dumps(samples)) + '}'
            output_str = ''
            try:
                output = ''
                s = socket.create_connection((host, port))
                try:
                    s.sendall(post_str.encode())
                    s.shutdown(socket.SHUT_WR)
                    buf = s.recv(4096)

                    while buf:
                        if len(output) < 16:
                            output += buf.decode()
                        # sys.stdout.write(buf.decode())
                        output_str += buf.decode()
                        buf = s.recv(4096)
                finally:
                    s.close()
            except:
                print('connecting host error!')

            if not output.startswith('SUCCESS\n'):
                sys.exit(1)

            print(output_str[8:])
            return output_str[8:]

    def post_process_trajectory(self, args):
        print('here is in post_process: ', args)

    def get_trajectories(self, input_dir, regex):
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


app = Trajectory()
app.main(input_dir='sanfrancisco/dataset/',
     regex='.txt',
     output='sanfrancisco/trajectory/sanfrancisco.trajectory',
     threads=1,)