from collections import defaultdict
import json
import sys
import traceback

pymap = map

def enum_paths(dct, path=[]):
  if not hasattr(dct, 'items'):
    path.append(dct)
    yield path
    return
  for k,v in dct.iteritems():
    for p in enum_paths(v, path + [k]):
      yield p

def map(k, d, v, cx):
  payload_out = []
  try:
    j = json.loads(v)
    if not "simpleMeasurements" in j:
      return
    s = j["simpleMeasurements"]
    if not "UITelemetry" in s:
      return
    sysinfo = j["info"]
    ui = s["UITelemetry"]
    countableEventBuckets = []
    customizeBuckets = []

    prefix = sysinfo["OS"]

    tour_seen = "none"

    toolbars = ui["toolbars"] 
    if not "menuBarEnabled" in toolbars: #remove weird incomplete cases
      return
    countableEvents = toolbars.get("countableEvents", {})
    feature_measures = {}
    #note: simple swaps in "kept"
    feature_measures["features_kept"] = toolbars.get("defaultKept",[])
    feature_measures["features_moved"] = toolbars.get("defaultMoved",[])
    feature_measures["extra_features_added"] = toolbars.get("nondefaultAdded", [])
    feature_measures["features_removed"] = toolbars.get("defaultRemoved", [])
  
    if "UITour" in ui:
      for tour in ui["UITour"]["seenPageIDs"]:
        tour_seen = tour
        payload_out.append((prefix + "," + "seenPage-" + tour, 1))
        #TODO: error checking on more than one tour

    for k,v in toolbars.iteritems():
      if k not in ["defaultKept", "defaultMoved", "nondefaultAdded", "defaultRemoved", "countableEvents", "durations"]:
        v = str(v)
        v = v.replace(",", " ") #remove commas in the tab arrays that will mess us up later
        payload_out.append((prefix + ","+ k + "-" + str(v), 1))

    bucketDurations = defaultdict(list)
    durations = toolbars.get("durations",{}).get("customization",[])

    for e in durations:
        #correct for addition of firstrun buckets
        if type(e) is dict:
          bucketDurations[e["bucket"]].append(e["duration"])
        #if the default bucket is "__DEFAULT__", no tour has been seen
        else:
          bucketDurations["none"].append(e)


    for d,l in bucketDurations.items():
        bucket = "none" if d == "__DEFAULT__" else d
        for i in l:
          payload_out.append((prefix + ","+ "customization_time", i))

    #record the locations and movement of the customization items
    #write out entire set for a user(dist), 
    #and also each individual item
    for e,v in feature_measures.items():
        for item in v:
          payload_out.append((prefix + "," + e+"-"+item, 1))

    #this will break pre-Australis
    bucketless_events = defaultdict(int)
    for i in countableEvents.values():
      for event_string in enum_paths(i,[]):
        bucketless_events["-".join(event_string[0:-1])] += int(event_string[-1])


    for event_string,val in bucketless_events.items():
      payload_out.append((prefix+ "," + event_string, val))



    #We haven't errored out! Now we can write everything.

    cx.write(prefix+ ",instances", 1)
    for tup in payload_out:
      a,b = tup
      cx.write(a,b)    

  except Exception, e:
    print >> sys.stderr, "ERROR:", e
    print >> sys.stderr, traceback.format_exc()
    cx.write("ERROR:", str(e))

def reduce(k, v, cx):
  if k == "JSON PARSE ERROR:":
    for i in set(v):
      cx.write(k, i)
    return
  cx.write(k + " array", v)
  # cx.write(k + " count", len(v))
  # cx.write(k + " sum", sum(pymap(float,v)))