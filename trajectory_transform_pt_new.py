import time
import json
import socket
from multiprocessing import Pool

host = ['172.19.7.241',]
port = '1234'
output_format = 'debug'


def process_trajectory(tid, tra_points, host, port, output_format, output_file):

    samples = tra_points

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

    recieve = ''
    recieve += output_str[8:-1]
    match_result = json.loads(recieve.split('\n')[-1])

    return (match_result, output_file + '_new_' + tid)


def post_process_trajectory(args):
    (result, output) = args
    print('Here is in post_process:', len(result))
    if len(result) < 1:
        return
    with open(output, 'w') as f:
        f.write(json.dumps(result))
    print('Post_process Done!')


def get_trajectories(input_file):

    request_file = open('porto/request/train.request', 'w+')

    with open(input_file, 'r') as trajectories:
        for line in trajectories:
            trajectory = []

            if line.startswith('"TRIP_ID"'):
                continue

            line_items = line.strip().split(',', 8)

            cleaner_items = [ele.strip('"') for ele in line_items]

            start_time = int(cleaner_items[5])
            tra_points = json.loads(cleaner_items[8])

            tra_size = len(tra_points)
            if tra_size < 30:
                continue

            for idx, point in enumerate(tra_points):
                point_time = idx * 15 + start_time
                position = 'POINT(' + str(round(point[0], 5)) + ' ' + str(round(point[1], 5)) + ')'
                point = {
                    "point": position,
                    "time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(point_time)) + '+0800',
                    "id": "1"
                }
                trajectory.append(point)
            request_file.write(cleaner_items[0] + ', ' + json.dumps(trajectory) + '\n')
            yield (cleaner_items[0], trajectory)


def main(input_file, output_file, threads):

    pool = Pool(threads)

    trajectories = get_trajectories(input_file)

    for idx, trajectory in enumerate(trajectories):
        host_idx = 0
        (tid, tra_points) = trajectory
        pool.apply_async(func=process_trajectory,
                         args=(tid, tra_points, host[host_idx], port, output_format, output_file),
                         callback=post_process_trajectory)
    pool.close()
    pool.join()


main(input_file='porto/dataset/train.csv',
     output_file='porto/trajectory/pt_tra',
     threads=2, )
