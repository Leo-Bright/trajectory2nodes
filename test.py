import json
with open('sanfrancisco/trajectory/sanfrancisco.trajectory_part534', 'r') as f :
    obj = json.loads(f.readline())

newobjList = []
for item in obj:
    newobj = {}
    newobj['road'] = item['road']
    newobj['time'] = item['time']
    newobj['heading'] = item['heading']
    newobj['frac'] = item['frac']
    newobjList.append(newobj)

print(len(newobjList))