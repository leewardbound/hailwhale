import json, time, sys, psutil, urllib
interval = 1

hw_host = 'localhost:8085'
pk = 'system_status'
def post_health_info():
    dimensions = { 'platforms': sys.platform }
    metrics = {
        'CPU percent': psutil.cpu_percent(),
        'RAM (GB)': psutil.phymem_usage().percent,
        'HD percent': psutil.disk_usage('/').percent,
    }
    url = 'http://%s/count_now?pk=%s&dimensions=%s&metrics=%s'%(
        hw_host, pk, json.dumps(dimensions), json.dumps(metrics))
    print 'GET:',url
    data = urllib.urlopen(url).read()
    print data
    try: 
        json.loads(data)
        if 'cmds' in data and data['cmds']:
            os.system(data['cmds'])
    except Exception,e: print e


if __name__ == '__main__':
    while True:
        post_health_info()
        time.sleep(interval)
