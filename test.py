import os
files = os.listdir('sanfrancisco/trajectory/')
for i, file in enumerate(files):
    if i == 5:
        break
    print(file)
