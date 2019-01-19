import pandas as pd
import time
import json
import os
import socket
from multiprocessing import Pool


host = ['172.19.7.235', '172.19.7.237', '172.19.7.238', '172.19.7.239', '172.19.7.240', '172.19.7.241', '172.19.7.242']
port = '1234'
output_format = 'debug'


def process_trajectory(tid, trajectory, host, port, output_format, output_file):
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
        match_result = json.loads(recieve.split('\n')[-1])
        all_match_result += match_result
    return (all_match_result, output_file + '_' + trajectory_file)


def post_process_trajectory(args):
    result, output = args
    print('Here is in post_process:')
    with open(output, 'a') as f:
        f.write(json.dumps(result))
    print('Post_process Done!')


def get_trajectories(input_file):

    with open(input_file, 'r') as trajectories:
        for line in trajectories:
            if line.startswith('"TRIP_ID"'):
                continue
            line_items = line.strip().split(',', 8)
            cleaner_items = [ele.strip('"') for ele in line_items]
            tra_size = len(cleaner_items)
            if tra_size < 60:
                continue
            yield (cleaner_items[0], json.loads(cleaner_items[8]))


def main(input_file, output_file, threads):

    pool = Pool(threads)

    trajectories = get_trajectories(input_file)

    for idx, trajectory in enumerate(trajectories):
        host_idx = idx % 7
        pool.apply_async(func=process_trajectory,
                         args=(trajectory[0], trajectory[1], host[host_idx], port, output_format, output_file),
                         callback=post_process_trajectory)
    pool.close()
    pool.join()


main(input_file='porto/dataset/test.csv',
     output_file='porto/trajectory/porto.trajectory',
     threads=14, )