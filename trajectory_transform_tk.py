import time
import json
import socket
import os
from multiprocessing import Pool


host = ['172.19.7.235', '172.19.7.237', '172.19.7.238', '172.19.7.239', '172.19.7.240', '172.19.7.241', '172.19.7.242']
port = '1234'
output_format = 'debug'


def process_trajectory(tid, tra_points, host, port, output_format, output_file):

    split_size = 200

    all_match_result = []
    part_count = len(tra_points) // split_size
    for index in range(part_count + 1):
        start = index * split_size
        if index == part_count:
            end = len(tra_points)
        else:
            end = (index + 1) * split_size
        samples = tra_points[start:end]
        # tmp = "batch-%s" % random.randint(0, sys.maxsize)

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
        all_match_result += match_result

    return (all_match_result, output_file + '_new_' + tid)


def post_process_trajectory(args):
    (result, output) = args
    print('Here is in post_process:')
    with open(output, 'a') as f:
        f.write(json.dumps(result))
    print('Post_process Done!')


def time_str2time_stamp(str):
    time_array = time.strptime(str, "%Y-%m-%d %H:%M:%S")
    time_stamp = int(time.mktime(time_array))
    return time_stamp


def get_trajectories(input_dir, regex):
    trajectory_files = []
    file_names = os.listdir(input_dir)
    for file_name in file_names:
        if file_name.find(regex) >= 0:
            trajectory_files.append(file_name)

    for trajectory_file in trajectory_files:

        id2trajectory = {}

        with open(input_dir + trajectory_file, 'r') as input_data:
            for line in input_data:

                line_items = line.strip().split(',')

                tra_id, tra_time, longitude, latitude, transport, weigh = line_items

                if tra_id not in id2trajectory:
                    id2trajectory[tra_id] = [(tra_time, longitude, latitude, transport), ]
                else:
                    id2trajectory[tra_id].append((tra_time, longitude, latitude, transport))

        trajectories = []
        for tra_id in id2trajectory:

            tra_points = id2trajectory[tra_id]

            trajectory = []
            for point in tra_points:

                if len(trajectory) == 0:
                    trajectory.append(point)
                    continue

                if point[3] == trajectory[-1][3]:
                    trajectory.append(point)
                    continue

                if len(trajectory) <= 10:
                    trajectory.clear()
                    continue

                if time_str2time_stamp(trajectory[-1][0]) - time_str2time_stamp(trajectory[0][0]) > 80:
                    trajectories.append(trajectory)

                trajectory.clear()
                trajectory.append(point)

            if len(trajectory) > 10 and time_str2time_stamp(trajectory[-1][0]) - time_str2time_stamp(trajectory[0][0]) > 80:
                trajectories.append(trajectory)

        for idx, traj in enumerate(trajectories):
            request_trajectory = []
            for gps_point in traj:
                position = 'POINT(' + str(gps_point[1]) + ' ' + str(gps_point[2]) + ')'
                request_point = {
                    "point": position,
                    "time": gps_point[0] + '+0800',
                    "id": "1"
                }
                request_trajectory.append(request_point)
        yield (trajectory_file + '_' + str(idx), request_trajectory)


def main(input_dir, regex, output_file, threads):

    pool = Pool(threads)

    trajectories = get_trajectories(input_dir, regex)

    for idx, id_trajectory in enumerate(trajectories):
        host_idx = idx % 7
        pool.apply_async(func=process_trajectory,
                         args=(id_trajectory[0], id_trajectory[1], host[host_idx], port, output_format, output_file),
                         callback=post_process_trajectory)
    pool.close()
    pool.join()


# print(time_str2time_stamp('2016-12-16 03:00:00'))
main(input_dir='tokyo/dataset/',
     regex='x0',
     output_file='tokyo/trajectory/tk_tra',
     threads=14, )