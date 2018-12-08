import psycopg2

# 数据库连接参数
conn = psycopg2.connect(database="porto", user="osmuser", password="pass", host="localhost", port="5432")
cur = conn.cursor()

cur.execute("select id, nodes from ways;")
rows = cur.fetchall()        # all rows in table

way2nodes = {}  # {osmid_way: [osmid_node,...]}
for row in rows:
    way2nodes[row[0]] = row[1]
conn.commit()
cur.close()
conn.close()

f_500K_ways = open(r'500k.w2v.txt', 'r')
f_500K_nodes = open(r'500k.nodes.txt', 'w+')
for line in f_500K_ways.readlines():
    output_nodes = []
    way_list = line.strip().split(" ")
    if len(way_list) < 1: continue
    for way in way_list:
        way = way.strip()
        if len(way) < 1 : continue
        osmid_way, heading = way.split("-")
        if int(osmid_way) not in way2nodes: continue
        nodes = way2nodes[int(osmid_way)]
        if heading == "forward":
            for node in nodes:
                output_nodes.append(node)
        else:
            index = len(nodes) - 1
            while index >= 0:
                node = nodes[index]
                output_nodes.append(node)
    f_500K_nodes.write(str(output_nodes) + '\n')

f_500K_ways.close()
f_500K_nodes.close()