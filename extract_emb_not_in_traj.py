def main(shortest_emb, trajectory_emb,output_node, output_emb):

    _shortest_nodes = get_node_set(trajectory_emb)
    _extra_nodes = set()

    with open(output_emb, 'w+') as output:
        with open(shortest_emb, ) as emb_file:
            for line in emb_file:
                node = line.strip().split(' ')[0]
                if node not in _shortest_nodes:
                    _extra_nodes.add(node)
                    output.write(line + "\n")

    with open(output_node, 'w+') as output_node:
        for node in _extra_nodes:
            output_node.write(node + '\n')


def get_node_set(trajectory_emb):

    nodes = set()

    with open(trajectory_emb) as emb_file:
        for line in emb_file:
            nodes.add(line.strip().split(' ')[0])
        return nodes


if __name__ == '__main__':

    main(shortest_emb='../my_model_distance_window/sanfrancisco/sf_trajectory_increament_tag_type_window_width500_crossing_node_38g.embedding',
         trajectory_emb='../my_model_distance_window/sanfrancisco/sf_trajectory_increament_tag_type_window_width500_crossing_node.embedding',
         output_node='sanfrancisco/sequence/sf_not_in_trajectory.node',
         output_emb='../my_model_distance_window/sanfrancisco/sf_not_in_trajectory_window_width500_crossing_node.embedding')