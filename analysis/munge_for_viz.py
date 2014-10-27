import simplejson as json
import sys
import traceback
import fileinput
from collections import defaultdict

'''
//For customization time:

{
"data_type": "customization_time",
"current_date": YYYY-MM-DD,
"previous_date": YYYY-MM-DD,
"values": [
{
"shortname": "blah blah blah",
"description": "This is average customization time in seconds for some subcategory",
"value": 123.23
},
{
"shortname": "blah blah blah",
"description": "This is average customization time in seconds on some other subcategory",
"value": 123.23
}
//...

]
}

//For states:

{
"data_type": "ui_state",
"current_date": YYYY-MM-DD,
"previous_date": YYYY-MM-DD,
"states": [
{
"shortname": "Menu button",
"description": "The menu button doohicky",
"values": [
{
"name": "Kept",
"current_count": 123,
"current_percent": 0.213
"previous_count": 63
"previous_percent": 0.094
},
{
"name": "Moved",
"current_count": 12,
"current_percent": 0.023
"previous_count": 6
"previous_percent": 0.0094
},
//...
]
}
//...
{
"shortname": "Bookmarks toolbar",
"description": "The bookmarks toolbar",
"values": [
{
"name": "Visible",
"current_count": 123,
"current_percent": 0.213
"previous_count": 63
"previous_percent": 0.094
},
{
"name": "Hidden",
"current_count": 12,
"current_percent": 0.023
"previous_count": 6
"previous_percent": 0.0094
},
{
"name": "Superposition",
"current_count": 12,
"current_percent": 0.023
"previous_count": 6
"previous_percent": 0.0094
},
//...
]
}
//...
]
}

//For actions

{
"data_type": "ui_actions",
"current_date": YYYY-MM-DD,
"previous_date": YYYY-MM-DD,
"actions": [
{
"shortname": "Menu button",
"description": "The menu button doohicky",
"current_count": 123,
"current_percent": 0.213,
"current_apu": 32.0231 // Actions per user
"previous_count": 63
"previous_percent": 0.094
"previous_apu": 32.0231 // Actions per user
},
{
"shortname": "Menu button",
"description": "The menu button doohicky",
"current_count": 123,
"current_percent": 0.213,
"current_apu": 32.0231, // Actions per user
"previous_count": 63,
"previous_percent": 0.094,
"previous_apu": 32.0231 // Actions per user
},
//...
]
}
'''

def enum_paths(dct, path=[]):
  if not hasattr(dct, 'items'):
    path.append(dct)
    yield path
    return
  for k,v in dct.iteritems():
    for p in enum_paths(v, path + [k]):
      yield p


cust_times_arr = defaultdict(list)
actions_arr = {"total": defaultdict(int), "users": defaultdict(int)}
states_arr = defaultdict(lambda: defaultdict(int))
instances = 0

for line in fileinput.input():
  try:
    #customization times
    blob = json.loads(line)
    cust_times = blob["uitelemetry"]["toolbars"]["durations"]["customization"]
    for c in cust_times:
      cust_times_arr[c["bucket"]].append(c["duration"])

    #feature states
    for key in ["defaultKept", "nondefaultAdded", "defaultMoved", "defaultRemoved"]: 
      #0 paddings??
      items = blob["uitelemetry"]["toolbars"][key]
      for i in items:
        states_arr[i][key] += 1

    #actions
    events = blob["uitelemetry"]["toolbars"]["countableEvents"]
    for bucket in events:
      for p in enum_paths(events[bucket]):
        actions_arr["total"]['-'.join(p[0:-1])] += p[-1]
        actions_arr["users"]['-'.join(p[0:-1])] += 1

    #other states

    instances += 1 
  except Exception as e:
    # print "Exception: ", type(e), e
    pass



#print cust_times_arr
print states_arr
# print actions_arr
# print instances











