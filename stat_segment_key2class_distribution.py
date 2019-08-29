import json

with open('seattle/dataset/all_road_segments_dict.seattle') as f:
    all_road_segments = json.loads(f.readline())

stat = {}
with open('seattle/dataset/seattle_segments_tiger_name_type.json') as f:
    nametype_dict = json.loads(f.readline())
    for seg in nametype_dict:
        name_type = nametype_dict[seg]
        if name_type not in stat:
            stat[name_type] = {}
        class_id_stat = stat[name_type]
        class_id = all_road_segments[str(seg)]['class_id']
        if class_id not in class_id_stat:
            class_id_stat[class_id] = 1
        else:
            class_id_stat[class_id] += 1

with open('seattle/dataset/seattle_classid_nametype_stat.info', 'w+') as f:
    for type in stat:
        class_id_stat = stat[type]
        for class_id in class_id_stat:
            count = class_id_stat[class_id]
            f.write(type + '\t' + str(class_id) + '\t' + str(count) + '\n')

