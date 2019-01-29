import time
import json
import socket
import os
from multiprocessing import Pool


host = ['172.19.7.235', '172.19.7.236', '172.19.7.237', '172.19.7.238', '172.19.7.239', '172.19.7.240', '172.19.7.241', '172.19.7.242']
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
    print('Here is in post_process: ', len(result))
    if len(result) < 1:
        return
    with open(output, 'w') as f:
        f.write(json.dumps(result))
    print('Post_process Done!')


def time_str2time_stamp(s):
    time_array = time.strptime(s, "%Y-%m-%d %H:%M:%S")
    time_stamp = int(time.mktime(time_array))
    return time_stamp


def get_requests(input_dir, regex):
    trajectory_files = []
    file_names = os.listdir(input_dir)
    for file_name in file_names:
        if file_name.find(regex) >= 0:
            trajectory_files.append(file_name)

    f1 = open('tokyo/request/transport_1.request', 'w+')
    f2 = open('tokyo/request/transport_2.request', 'w+')
    f3 = open('tokyo/request/transport_3.request', 'w+')
    f4 = open('tokyo/request/transport_4.request', 'w+')

    trajectory_total = 0
    travel_time_total = 0
    count_point_1 = 0
    count_point_2 = 0
    count_point_3 = 0
    count_point_4 = 0
    count_point_99 = 0
    count_point_lt5 = 0

    for trajectory_file in trajectory_files:

        id2trajectory = {}

        with open(input_dir + trajectory_file, 'r') as input_data:
            for line in input_data:

                line_items = line.strip().split(',')

                if len(line_items) != 6:
                    print(line_items)
                    continue

                tra_id, tra_time, longitude, latitude, transport, weigh = line_items

                if tra_id not in id2trajectory:
                    id2trajectory[tra_id] = [(tra_id, tra_time, longitude, latitude, transport), ]
                else:
                    id2trajectory[tra_id].append((tra_id, tra_time, longitude, latitude, transport))

        trajectories = []

        for tra_id in id2trajectory:

            tra_points = id2trajectory[tra_id]

            trajectory = []
            for point in tra_points:

                if len(trajectory) == 0:
                    trajectory.append(point)
                    continue

                if point[4] == trajectory[-1][4]:
                    trajectory.append(point)
                    continue

                if len(trajectory) >= 5:
                    trajectories.append(trajectory)
                    trajectory = []
                    trajectory.append(point)
                else:
                    count_point_lt5 += len(trajectory)
                    trajectory = []
                    trajectory.append(point)

            if len(trajectory) >= 5:
                trajectories.append(trajectory)
            else:
                count_point_lt5 += len(trajectory)

        for idx, traj in enumerate(trajectories):

            trans_tool = traj[0][4]
            tra_travel_time = time_str2time_stamp(traj[-1][1]) - time_str2time_stamp(traj[0][1])
            tra_size = len(traj)

            if trans_tool == '99':
                count_point_99 += tra_size
                continue

            request_trajectory = []

            for gps_point in traj:
                position = 'POINT(' + str(gps_point[2]) + ' ' + str(gps_point[3]) + ')'
                request_point = {
                    "point": position,
                    "time": gps_point[1] + '+0800',
                    "id": "1"
                }
                request_trajectory.append(request_point)

            if trans_tool == '1':
                count_point_1 += tra_size
                f1.write(trajectory_file + '_' + str(idx) + ', ' + json.dumps(request_trajectory) + '\n')
            if trans_tool == '2':
                count_point_2 += tra_size
                trajectory_total += 1
                travel_time_total += tra_travel_time
                f2.write(trajectory_file + '_' + str(idx) + ', ' + json.dumps(request_trajectory) + '\n')
            if trans_tool == '3':
                count_point_3 += tra_size
                f3.write(trajectory_file + '_' + str(idx) + ', ' + json.dumps(request_trajectory) + '\n')
            if trans_tool == '4':
                trajectory_total += 1
                travel_time_total += tra_travel_time
                count_point_4 += tra_size
                f4.write(trajectory_file + '_' + str(idx) + ', ' + json.dumps(request_trajectory) + '\n')
    f1.close()
    f2.close()
    f3.close()
    f4.close()
    print('99:', count_point_99)
    print('1:', count_point_1)
    print('2:', count_point_2)
    print('3:', count_point_3)
    print('4:', count_point_4)
    print('lt5:', count_point_lt5)
    print('time_2_4: ', travel_time_total)
    print('traj_2_4: ', trajectory_total)


def statistical(input_file):

    count_20, count_50, count_100, count_200, count_400, count_400up = [0, 0, 0, 0, 0, 0]

    file_size = 0
    with open(input_file, 'r') as f:
        for line in f:
            file_size += 1
            tid, request_points = line.strip().split(',', 1)
            request = json.loads(request_points)
            size = len(request)
            if size < 15:
                count_20 += 1
            elif size < 50:
                count_50 += 1
            elif size < 120:
                count_100 += 1
            elif size < 200:
                count_200 += 1
            elif size < 400:
                count_400 += 1
            else:
                count_400up += 1
    print(file_size)
    print([count_20, count_50, count_100, count_200, count_400, count_400up])


def process_request(request_file):

    with open(request_file, 'r') as f:
        for line in f:
            tid, request_points = line.strip().split(',', 1)
            request = json.loads(request_points)
            gps_size = len(request)
            if gps_size < 5:
                continue
            yield (tid, request)


def main(input_dir, regex, output_file, threads):

    # pool = Pool(threads)

    get_requests(input_dir, regex)

    # statistical('tokyo/request/transport_2.request')

    # trajectories = process_request('tokyo/request/transport_2.request')

    # for idx, tid_trajectory in enumerate(trajectories):
    #     host_idx = idx % 8
    #     pool.apply_async(func=process_trajectory,
    #                      args=('transport_2_' + tid_trajectory[0], tid_trajectory[1], host[host_idx], port, output_format, output_file),
    #                      callback=post_process_trajectory)
    # pool.close()
    # pool.join()


main(input_dir='tokyo/dataset/',
     regex='x0',
     output_file='tokyo/trajectory/tk_tra',
     threads=16, )
