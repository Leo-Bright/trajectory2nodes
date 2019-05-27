max_length = 0
max_id = 0
min_length = 999
min_id = 0

with open('porto/sequence/pt_trajectory_node_travel_time_50_70_1w.travel') as f:
    for idx, line in f:
        nodes_time = line.strip().split(' ')
        size = len(nodes_time)
        if size > max_length:
            max_length = size
            max_id = idx
        if size < min_length:
            min_length = size
            min_id = idx

print("max_length: ", max_length)
print("max_id: ", max_id)
print("min_length: ", min_length)
print("min_id: ", min_id)