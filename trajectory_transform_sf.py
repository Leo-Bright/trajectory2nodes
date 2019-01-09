import pandas as pd
import time
import json

data_matrix = pd.read_csv("sanfrancisco/dataset/new_abboip.txt", header=None, sep=' ', index_col=None)
latitude = data_matrix[0]
longitude = data_matrix[1]
occupancy = data_matrix[2]
time_gps = data_matrix[3]

new_data_matrix = pd.DataFrame()
new_data_matrix['longitude'] = longitude
new_data_matrix['latitude'] = latitude
new_data_matrix['occupancy'] = occupancy
new_data_matrix['time'] = time_gps
print(new_data_matrix)

trajectory = []

for row in new_data_matrix.itertuples():
    print(round(getattr(row, 'longitude'), 5), round(getattr(row, 'latitude'), 5))
    position = 'POINT(' + str(round(getattr(row, 'longitude'), 5)) + ' ' + str(round(getattr(row, 'latitude'), 5)) + ')'
    point = {
        "point": position,
        "time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(getattr(row, 'time')))) + '+0200',
        "id": "\\x0001"
    }
    trajectory.append(point)

with open('sanfrancisco/trajectory/new_abboip.json', 'w+') as output:
    output.write(json.dumps(trajectory))
