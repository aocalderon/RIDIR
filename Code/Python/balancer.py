#!/opt/Anaconda3/bin/python3

import sys, os, getopt, json, urllib
from urllib.request import urlopen
import pandas as pd

def getExecutorTimes(server, port, appId, stageId):
    url = "http://{}:{}/history/{}/stages/stage/?id={}&attempt=0".format(server, port, appId, stageId)
    print("Contacting {} ...".format(url))
    data = pd.read_html(url)
    executorTimes = data[1][['Address', 'Task Time']]
    executorTimes = executorTimes.assign(time = executorTimes['Task Time'].apply(parseTime))
    executorTimes = executorTimes.assign(executor = executorTimes['Address'].apply(parseAddress))
    n = len(executorTimes.index)
    executorTimes = executorTimes.assign(appId = [appId] * n)
    executorTimes = executorTimes.assign(stageId = [stageId] * n)
    return executorTimes[['appId', 'stageId', 'executor', 'time']]

def getAppIds(server, port, name_, from_, to_, user="acald013"):
    request  = urllib.request.Request("http://{}:{}/api/v1/applications".format(server, port))
    response = urllib.request.urlopen(request).read()
    history  = json.loads(response.decode('utf-8'))
    # Getting data from server...
    apps = [ [history[i]['id'], history[i]['name'], history[i]['attempts'][0]['sparkUser'] ] for i in range(len(history)) ]
    apps = pd.DataFrame(apps, columns = ['id', 'name', 'user'])
    n = len(apps.index)
    arr = apps['id'].str.split("_")
    apps = apps.assign(m = len(arr[0]))    
    apps = apps.assign(count = apps['id'].str.split("_", expand = False).apply(lambda x:x[2]).astype(int))
    # Filtering data...
    apps = apps[(apps.name.str.contains(name_)) & (apps['user'] == user) & (apps.m == 3) & (apps['count'] >= from_) & (apps['count'] <= to_)]

    return apps

def parseTime(time_):
    arr = time_.split(" ")
    t = float(arr[0])
    if arr[1] == "min":
        t = t * 60.0
    return t

def parseAddress(address_):
    arr = address_.split(":")
    return arr[0]

def main(argv):
    server = "localhost"
    port = "18080"
    appId = ""
    stageId = "0"
    name = "\*"
    from_ = 0
    to_ = 10
    input_ = ""
    output = "/tmp/output.tsv"
    try:
        opts, args = getopt.getopt(argv,"hs:p:a:g:n:f:t:i:o:",["server=", "port=", "aid=", "sid=", "name=", "from=", "to=","input=", "output="])
    except getopt.GetoptError:
        command = './balancer.py -s <server> -p <port> -a <appId> -g <stageId> -n <name> -f <from> -t <to> -i <input> -o <output>'
        print(command)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(command)
            sys.exit()
        elif opt in ("-s", "--server"):
            server = arg
        elif opt in ("-p", "--port"):
            port = arg
        elif opt in ("-a", "--aid"):
            appId = arg
        elif opt in ("-g", "--sid"):
            stageId = arg
        elif opt in ("-n", "--name"):
            name = arg
        elif opt in ("-f", "--from"):
            from_ = int(arg)
        elif opt in ("-t", "--to"):
            to_ = int(arg)
        elif opt in ("-i", "--input"):
            input_ = arg
        elif opt in ("-o", "--output"):
            output = arg

    apps = getAppIds(server, port, name, from_, to_)
    if os.path.exists(output):
        os.remove(output)
    for index, app in apps.iterrows():
        aid  = app['id']
        name = app['name']
        print("Processing {} [{}]...".format(aid, name))
        executorTimes = getExecutorTimes(server, port, aid, stageId)
        print(executorTimes)
        executorTimes.to_csv(output, sep='\t', index = False, header = False, mode='a')

if __name__ == "__main__":
    main(sys.argv[1:])

