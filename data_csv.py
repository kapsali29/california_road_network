import urllib
import pandas as pd
from pandas import read_csv
#download data and save them to csv from url https://www.cs.utah.edu/~lifeifei/research/tpq/cal.cnode
link = "https://www.cs.utah.edu/~lifeifei/research/tpq/cal.cnode"
f = urllib.request.urlopen(link)
mfile = f.read()
with open('nodes.csv', 'wb') as csvfile:
	csvfile.write(mfile)
df = read_csv('nodes.csv',header=None, delimiter=' ')
df.columns = ['node_id', 'long', 'lat']
df.to_csv('nodes.csv',index=False)

link = "https://www.cs.utah.edu/~lifeifei/research/tpq/calmap.txt"
f = urllib.request.urlopen(link)
categ = []
cat1 = []
sn = ' '
en = ' '
for item in f:
	temp = item.decode("utf-8").split()
	if (len(temp) ==4 and len(temp[3])==1) or (len(temp) ==4 and len(temp[3])==2):
		sn = temp[0]
		en = temp[1]
		#nodes.append([temp[0],temp[1]])
		categ.append(item.decode("utf-8").split())
	else:
		i = 0
		while(i<=len(temp) - 2):
			t = []
			t = [temp[i], temp[i+1]]
			t.append(sn)
			t.append(en)
			cat1.append(t)
			i = i + 2
df1 = pd.DataFrame(categ)
df1.columns = ['start_node','end_node', 'distance', 'num_pois']
#print(df1)
df3 = pd.DataFrame(cat1)
df3.columns = ['category_id', 'distance_from_start', 'start_node', 'end_node']

link = 'https://www.cs.utah.edu/~lifeifei/research/tpq/cal.cedge'
f = urllib.request.urlopen(link)
categ = []
for item in f:
	categ.append(item.decode("utf-8").split())
df2 = pd.DataFrame(categ)
df2.columns = ['edge_id','start_node','end_node', 'distance']

data_merged = pd.merge(df2,df1, how='right', on=['start_node','end_node'])
data_merged = data_merged.dropna()
data_merged['distance'] = data_merged['distance_x']
del data_merged['distance_x']
del data_merged['distance_y']
data_merged.to_csv('edges.csv',index=False)

merged_pois = pd.merge(df2,df3, how='right', on=['start_node','end_node'])
del merged_pois['distance']
merged_pois = merged_pois.dropna()
merged_pois['id'] = merged_pois.index 
merged_pois.to_csv('pois.csv',index=False)

link = "https://www.cs.utah.edu/~lifeifei/research/tpq/CA"
f = urllib.request.urlopen(link)
catt1 = []
for item in f:
	temp = item.decode('utf8').split()
	if len(temp) == 3:
		temp[1] = round(float(temp[1]),5)
		temp[2] = round(float(temp[2]),5)
		catt1.append(temp)
	else:
		continue
df5 = pd.DataFrame(catt1)
df5.columns = ['category_name', 'long', 'lat']

link = "https://www.cs.utah.edu/~lifeifei/research/tpq/caldata"
f = urllib.request.urlopen(link)
catt2 = []
for item in f:
	temp = item.decode('utf8').split()
	if len(temp) == 3:
		temp[0] = round(float(temp[0]),5)
		temp[1] = round(float(temp[1]),5)
		catt2.append(temp)
	else:
		continue
df6 = pd.DataFrame(catt2)
df6.columns = ['long', 'lat', 'category_id']

merged1 = pd.merge(df6,df5, how='right', on=['long','lat'])
merged1 = merged1.dropna()
del merged1['long']
del merged1['lat']
merged1 = merged1.set_index('category_name').T.to_dict('list')
for i in merged1.keys():
	merged1[i] = merged1[i][0]
merged1 = pd.DataFrame(list(merged1.items()))
merged1.columns = ['category_name', 'category_id']
merged1.to_csv('categories.csv',index=False)



