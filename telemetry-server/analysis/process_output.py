'''
post-process what comes out of m/r job

usage: cat final_data/my_mapreduce_results_pre.out | python process_output.py

input: output from bucketless_uitour (NOT uitour!!!), ex:
Linux,features_kept-bookmarks-menu-button,array	[2, 3]
WINNT,click-builtin-item-preferences-button-left,array	[1]

output: csv w/ header listing
osinfo,item,distribution,n in os

***we are now guaranteed to have unique (prefix,item) output from bucketless_uitour. No more storing everything in memory! huzzah!
'''

import re, ast, json
from collections import defaultdict

#position data
PREFIX=0
ITEM=1
TYPE=2
ARRAY=3


def distn(lst):
	dist = defaultdict(int)
	for i in lst:
		dist[i] += 1
	return dict(dist)



def process_output(infile, outfile):
	from collections import defaultdict
	from pprint import pprint as pp
	import operator
	import csv

	instances = {}
	addonbar_counts = defaultdict(list)

	#iterate through file once to get os counts so that we append these to rows in final output
	
	filecontents = open(infile)
	for line in filecontents:
		tokens = re.split(',|\s', line, 3)
		if tokens[ITEM] == "instances":
			instances[tokens[PREFIX]] = int(sum(ast.literal_eval(tokens[ARRAY])))


	#we have to precomput these as well. in the future, maybe detect strings like this in the m/r step
		elif tokens[ITEM].startswith("addonToolbars"):
			name, n = tokens[ITEM].split("-")
			n = int(n)
			arr_len = len(ast.literal_eval(tokens[ARRAY]))
			addonbar_counts[tuple([tokens[PREFIX],"addonToolbars"])].extend([n for i in range(arr_len)])

	filecontents.close()
		

	with open(outfile, "w") as outfile:
		csvwriter = csv.writer(outfile)
		filecontents = open(infile) #restart stream

		csvwriter.writerow(["sys_info","item","full_counts", "n_in_os_group"])

		for line in filecontents:
			if line.startswith("ERROR"):
				continue

			tokens = re.split(',|\s', line, 3)
			
			if tokens[ITEM] == "instances": #we've gotten these already
				continue

			elif "visibleTabs" in tokens[ITEM] or "hiddenTabs" in tokens[ITEM]: #we might get these later
				continue

			elif tokens[ITEM].startswith("addonToolbars"): #gotten these already
				continue

			tokens[ARRAY] = ast.literal_eval(tokens[ARRAY])

			if tokens[ITEM] == "customization_time":
				sec_array = [] #round to the second
				for t in tokens[ARRAY]:
					sec_array.append(int(round(float(t)/1000)))
				csvwriter.writerow([tokens[PREFIX], tokens[ITEM], json.dumps(distn(sec_array)), instances[tokens[PREFIX]]])

			else:
				csvwriter.writerow([tokens[PREFIX], tokens[ITEM], json.dumps(distn(tokens[ARRAY])), instances[tokens[PREFIX]]])

			#now take care of addonbars calculated above
		

		for tup, cts in addonbar_counts.iteritems():
			csvwriter.writerow(list(tup) + [json.dumps(distn(cts)), instances[tup[0]]])

#number of windows as distn
#total number of tabs