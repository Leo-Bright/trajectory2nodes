import psycopg2
from gensim.models import Word2Vec
import json
import os


def get_inputs(dir):
    all_files = os.listdir(dir)
    for file in all_files:
        if file.find('sanfrancisco.trajectory_new') >= 0:
            with open(file, 'r') as f:
                yield json.loads(f.readline())


def main(input_dir, output):
    input_trajectories = get_inputs(input_dir)
    for trajectory in input_trajectories:
        new_trajectory = []
        for matched in trajectory:
            new_matched = {}
            if 'transition' not in trajectory:
                new_trajectory.append(matched['point'])
            else:
                pass













main(input_dir='sanfrancisco/trajectory/',
     output='sanfrancisco/road_sequence/sf_road_segment.sequence')