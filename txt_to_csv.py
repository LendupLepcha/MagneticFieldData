import pandas as pd
import numpy as np
def text_to_csv(filename):
	f = open(filename+".txt", "r")
	new_line = ''
	j = ''
	OBs = []
	for line in f:
		
		for i in line:
	
			new_line = new_line + j
			if j.isdigit():
				if i.isalpha():
					#print(new_line)
					OBs.append(new_line)
					new_line = ''
			j = i
	print('List created')
	obs_list = []
	for obs in OBs:
		temp = obs.split(',')
		obs_list.append(temp)
	print('Items split '+filename)
	
	obs_nd = np.array(obs_list)
	print('Converted to ndarray for '+filename)
	
	obs_pd = pd.DataFrame(obs_nd, columns = ['code', 'obs_name', 'coordinates'])
	print('Converted to DF '+filename)
	
	obs_pd.to_csv(filename+'.csv', index = False)
	print('Saved as CSV '+filename)

files = ['note1', 'note2', 'note3']
for i in files:
	text_to_csv(i)
	print(i, ' is done')