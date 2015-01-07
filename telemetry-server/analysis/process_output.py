'''
post-process what comes out of m/r job

usage: cat final_data/my_mapreduce_results_pre.out | python process_output.py

input: output from uitour, ex:
Linux,none,features_kept-bookmarks-menu-button,sum	2.0
WINNT,bucket_UITour|australis-tour-aurora-29.0a2,click-builtin-item-preferences-button-left,count	1

output: csv w/ header listing
osinfo,item,instances per session,percentage of sessions with occurrence

ex:
WINNT,none,bookmarksBarEnabled-False,194609.0,0.5405
Linux,tour-29,bookmarksBarEnabled-True,165417.0,0.4595
'''

import re, ast, json
from collections import defaultdict

#position data
PREFIX=0
BUCKET=1
ITEM=2
TYPE=3
ARRAY=4

def distn(lst):
	dist = defaultdict(int)
	for i in lst:
		dist[i] += 1
	return dict(dist)



def process_output(filecontents, outfile):
	from collections import defaultdict
	from pprint import pprint as pp
	import operator
	import csv

	counts = defaultdict(list)
	instances = {}
	addonbar_num_array = defaultdict(int)

	for line in filecontents:
		if line.startswith("ERROR"):
			continue

		tokens = re.split(',|\s', line, 4)
		if tokens[ITEM] == "instances":
			instances[tokens[PREFIX]] = int(sum(ast.literal_eval(tokens[ARRAY])))
			continue
		
		#special cases for bucketed counting
		if tokens[ITEM] == "customization_time":
			sec_array = []
			for t in ast.literal_eval(tokens[ARRAY]):
				sec_array.append(int(round(float(t)/1000)))
			# counts[tuple([tokens[PREFIX], tokens[ITEM]])] = sec_array
			continue


		elif tokens[ITEM].startswith("addonToolbars"):
			name, n = tokens[ITEM].split("-")
			n = int(n)
			arr_len = len(ast.literal_eval(tokens[ARRAY]))
			counts[tuple([tokens[PREFIX],"addonToolbars"])].extend([n for i in range(arr_len)])
			continue

		elif "visibleTabs" in tokens[ITEM] or "hiddenTabs" in tokens[ITEM]:
			continue

		counts[tuple([tokens[PREFIX], tokens[ITEM]])].extend(ast.literal_eval(tokens[ARRAY]))

	with open(outfile, "w") as outfile:
		csvwriter = csv.writer(outfile)
		csvwriter.writerow(["sys_info","item","full_counts", "n_in_os_group"])
		for tup, cts in counts.iteritems():
			output = list(tup)
			output.extend([json.dumps(distn(cts)), instances[tup[0]]])
			csvwriter.writerow(output)

if __name__ == '__main__':
	import fileinput
	import sys

	process_output(sys.stdout)

#number of windows as distn
#total number of tabs