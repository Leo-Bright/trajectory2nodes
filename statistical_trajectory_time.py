import json


def main(road_segment_file):

    trajectory_total_num = 0
    points_total_num = 0
    with open(road_segment_file, 'r') as f:
        for line in f:
            if line.startswith('"TRIP_ID"'):
                continue
            line_items = line.strip().split(',', 8)
            cleaner_items = [ele.strip('"') for ele in line_items]
            tra_points = json.loads(cleaner_items[8])
            tra_size = len(tra_points)
            if tra_size < 30:
                continue
            points_total_num += tra_size
            trajectory_total_num += 1

    print('trajectory_total_num: ', trajectory_total_num)
    print('points_total_num: ', points_total_num)
    print('average trajectory points: ', points_total_num/trajectory_total_num)


if __name__ == '__main__':

    main('porto/dataset/train.csv', )
