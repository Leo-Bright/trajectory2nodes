import pandas as pd
import time
import json
import os
import socket
from multiprocessing import Pool


host = ['172.19.7.235', '172.19.7.237', '172.19.7.238', '172.19.7.239', '172.19.7.240', '172.19.7.241', '172.19.7.242']
port = '1234'
output_format = 'debug'


def process_trajectory(trajectory_file, trajectory, host, port, output_format, output_file):
    all_match_result = []
    split_trajs = split_with_time(trajectory, 45)
    for traj in split_trajs:
        samples = traj
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
        match_result = json.loads(recieve.split('\n')[-1])
        all_match_result.append(match_result)
    return (all_match_result, output_file + '_' + trajectory_file)


def post_process_trajectory(args):
    all_result, output = args
    print('Here is in post_process:', output)
    with open(output, 'a') as f:
        for idx, result in enumerate(all_result):
            f.write(json.dumps(result) + '\n')
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

        yield (trajectory_file, trajectory[::-1])




def split_with_time(origin, seg):
    split_trajs = []
    traj = []
    traj.append(origin[0])
    for id in range(1, len(origin)):
        cur_point = origin[id]
        last_point = origin[id-1]
        cur_point_time = time.mktime(time.strptime(cur_point['time'][:-5], '%Y-%m-%d %H:%M:%S'))
        last_point_time = time.mktime(time.strptime(last_point['time'][:-5], '%Y-%m-%d %H:%M:%S'))
        if cur_point_time - last_point_time <seg:
            traj.append(cur_point)
        else:
            split_trajs.append(traj)
            traj = []
            traj.append(cur_point)
    return split_trajs


def main(input_dir, regex, output_file, threads):

    pool = Pool(threads)

    trajectories = get_trajectories(input_dir, regex)

    for idx, trajectory in enumerate(trajectories):
        # host_idx = random.randint(0, len(host) - 1)
        trajectory_file = trajectory[0]
        trajs = trajectory[1]
        host_idx = idx % 7
        pool.apply_async(func=process_trajectory,
                         args=(trajectory_file, trajs, host[host_idx], port, output_format, output_file),
                         callback=post_process_trajectory)
    pool.close()
    pool.join()


main(input_dir='sanfrancisco/dataset/',
     regex='.txt',
     output_file='sanfrancisco/trajectory/sanfrancisco.trajectory',
     threads=14, )
