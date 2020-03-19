import json
import glob

result = ""
for i in range(1,11):
    for f in glob.glob("./sample_data/wiki_"+str(i)+".json"):
        with open(f,'r') as file:
            # print(file.read())
            result+=file.read()
    

with open("merged_data.json", "w") as outfile:
     
     outfile.write(result)

     