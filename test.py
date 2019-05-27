max_length = 0

with open('porto/sequence/pt_trajectory_node_travel_time_50_70_1w.travel') as f:
    for line in f:
        nodes_time = line.strip().split(' ')
        size = len(nodes_time)
        if size > max_length:
            max_length = size

print("max_length: ", max_length)