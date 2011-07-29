import json, time, sys, psutil, urllib
interval = 1

hw_host = 'localhost:8080'
health_cat = 'system_status'
def post_health_info():
    dimensions = { 'platforms': sys.platform }
    metrics = {
        'CPU percent': psutil.cpu_percent(),
        'RAM percent': psutil.phymem_usage().percent,
        'HD percent': psutil.disk_usage('/').percent,
    }
    url = 'http://%s/count_now?categories=%s&dimensions=%s&metrics=%s'%(
        hw_host, health_cat, json.dumps(dimensions), json.dumps(metrics))
    print 'GET:',url
    print urllib.urlopen(url).read()

if __name__ == '__main__':
    while True:
        post_health_info()
        time.sleep(interval)
