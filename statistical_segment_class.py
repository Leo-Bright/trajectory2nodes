import json
import time
import os


def main(segment_file, class_file, output):

    segment_type_count = {}

    with open(segment_file, 'r') as f:
        for line in f:
            gid_type = line.strip().split(' ')
            gid, type = gid_type
            if type not in segment_type_count:
                segment_type_count[type] = 1
            else:
                segment_type_count[type] += 1

    config = {}
    with open(class_file, 'r') as f:
        jsondata = json.load(f)
        for tag in jsondata["tags"]:
            for value in tag["values"]:
                config[value["id"]] = (value["name"],
                                            value["priority"], value["maxspeed"])

    sorted_type_count = []
    for type in segment_type_count:
        item = (type, config[int(type)][0], segment_type_count[type])
        sorted_type_count.append(item)

    sorted_type_count.sort(key=lambda x: x[-1], reverse=True)

    with open(output, 'w+') as f:
        for item in sorted_type_count:
            f.write(item.__str__() + '\n')


if __name__ == '__main__':

    main(segment_file='tokyo/dataset/segment_types.tokyo',
         class_file='tools/road-types.json',
         output='tokyo/dataset/segment_classid.count')
