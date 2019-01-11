import pandas as pd
import time
import json
import os
import socket
from multiprocessing import Pool


host = ['172.19.7.239', '172.19.7.240', '172.19.7.241', '172.19.7.242', ]
port = '1234'
output_format = 'slimjson'


def process_trajectory(id, trajectory, host, port, output_format, output_file):
    all_match_result = []
    part_count = len(trajectory) // 200
    for index in range(part_count + 1):
        start = index * 200
        if index == part_count:
            end = len(trajectory)
        else:
            end = (index + 1) * 200
        samples = trajectory[start:end]
        # tmp = "batch-%s" % random.randint(0, sys.maxsize)
        # file = open(tmp, "w")
        post_str = '{' + '"format": {format}, "request": {samples}'.format(format=output_format, samples=json.dumps(samples)) + '}'
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
            # sys.exit(1)
            print('Pay attention here!!! Here is a bad match action!@@@!!!!')
            continue

        recieve = ''
        recieve += output_str[8:-1]
        match_result = json.loads(recieve)
        for index, obj in enumerate(match_result):
            obj['time'] = samples[index]['time']
        all_match_result += match_result
    return (all_match_result, output_file + '_part' + str(id))


def post_process_trajectory(args):
    result, output = args
    print('Here is in post_process:')
    with open(output, 'a') as f:
        f.write(json.dumps(result))
    print('Post_process Done!')

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
            position = 'POINT(' + str(round(getattr(row, 'longitude'), 5)) + ' ' + str(round(getattr(row, 'latitude'), 5)) + ')'
            point = {
                "point": position,
                "time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(getattr(row, 'time')))) + '+0800',
                "id": "1"
            }
            trajectory.append(point)

        yield trajectory[::-1]


def main(input_dir, regex, output_file, threads):

    pool = Pool(threads)

    trajectories = get_trajectories(input_dir, regex)

    for idx, trajectory in enumerate(trajectories):
        # host_idx = random.randint(0, len(host) - 1)
        host_idx = idx % 4
        pool.apply_async(func=process_trajectory,
                         args=(idx, trajectory, host[host_idx], port, output_format, output_file),
                         callback=post_process_trajectory)
    pool.close()
    pool.join()


main(input_dir='sanfrancisco/dataset/',
     regex='.txt',
     output_file='sanfrancisco/trajectory/sanfrancisco.trajectory',
     threads=4,)