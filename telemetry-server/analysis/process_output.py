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

def process_output(infile, outfile):
	from collections import defaultdict
	from pprint import pprint as pp
	import operator
	import csv

	instances = {}
	addonbar_counts = defaultdict(dict)

	#iterate through file once to get os counts so that we append these to rows in final output
	
	filecontents = open(infile)
	for line in filecontents:
		if line.startswith("ERROR"):
			continue

		tokens, distn = line.split('\t',1)
		distn = json.loads(distn) 
		prefix, val = tokens.split(',')
		if val == "instances":
			instances[prefix] = distn['1']


	#we have to precompute these as well. in the future, maybe detect strings like this in the m/r step
		elif val.startswith("addonToolbars"):
			name, n = val.split("-")
			addonbar_counts[prefix][n] = distn["1"]

	filecontents.close()
		

	with open(outfile, "w") as outfile:
		csvwriter = csv.writer(outfile)
		filecontents = open(infile) #restart stream

		csvwriter.writerow(["sys_info","item","full_counts", "n_in_os_group"])

		for line in filecontents:
			if line.startswith("ERROR"):
				continue

			tokens, distn = line.split('\t',1)
			distn = json.loads(distn) 
			prefix, val = tokens.split(',')
			
			if val == "instances": #we've gotten these already
				continue

			elif "visibleTabs" in val or "hiddenTabs" in val: #we might get these later
				continue

			elif val.startswith("addonToolbars"): #gotten these already
				continue

			else:
				csvwriter.writerow([prefix, val, json.dumps(distn), instances[prefix]])

		#now take care of addonbars calculated above
	
		for prefix in addonbar_counts:
			csvwriter.writerow([prefix, "addonToolbars", json.dumps(addonbar_counts[prefix]), instances[prefix]])

#number of windows as distn
#total number of tabs