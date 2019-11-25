def main(shortest_emb, trajectory_emb, output_emb):

    _ex_nodes = get_node_set(trajectory_emb)

    print("nodes in shortest and not in traj:", _ex_nodes)
    with open(output_emb, 'w+') as output:
        with open(shortest_emb, ) as emb_file:
            for line in emb_file:
                node = line.strip().split(' ')[0]
                if node not in _ex_nodes:
                    output.write(line + "\n")


def get_node_set(trajectory_emb):

    nodes = set()

    with open(trajectory_emb) as emb_file:
        for line in emb_file:
            nodes.add(line.strip().split(' ')[0])
        return nodes


if __name__ == '__main__':

    main(shortest_emb='../my_model_distance_window/porto/sequence/sf_trajectory_road_segment.sequence',
         trajectory_emb='porto/sequence/sf_trajectory_sequence.node',
         output_node='',
         output_emb='')