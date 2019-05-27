max_length = 0
min_length = 999

with open('porto/sequence/pt_trajectory_node_travel_time_50_70_1w.travel') as f:
    for line in f:
        nodes_time = line.strip().split(' ')
        size = len(nodes_time)
        if size > max_length:
            max_length = size
        if size < min_length:
            min_length = size

print("max_length: ", max_length)
print("min_length: ", min_length)