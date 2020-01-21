import sys, json, os

import docker
import requests

def start_image(client, name):
   run= False
   try:
    image=client.images.get(name)
    print(('image found:', image.id))
   except Exception:
    print(("downloading image ... ", name))
    image=client.images.pull(name)
    print(('image downloaded:', image.id))
   for container in client.containers.list(filters={"ancestor":name,"status":"running"}):
         run=True
   if not run:
       id=client.containers.run(name, ports={1080:1080}, detach=True)
       print(id)
   else:
       print("container is already running,hence resetting it")
       reset()

def stop_containar(client, name):
    for container in client.containers.list(filters={"ancestor":"jamesdbloom/mockserver:latest","status":"running"}):
        container.stop()

def setup():
    url='http://localhost:1080/mockserver/expectation'
    file = open('b/resources/eventing/curl_setup.txt')
    data = json.load(file)
    for d in data:
        response = requests.put(url, data=json.dumps(d))
        if response.status_code==201:
            pass
        else:
            raise Exception('erro occured: ', response.text)
    res=requests.put('http://localhost:1080/mockserver/retrieve?type=ACTIVE_EXPECTATIONS')
    print((json.dumps(res.json(), indent=4, sort_keys=True)))

def reset():
    url = 'http://localhost:1080/mockserver/reset'
    response = requests.put(url)
    if response.status_code == 200:
        pass
    else:
        raise Exception('error occured: ', response.text)
    res = requests.put('http://localhost:1080/mockserver/retrieve?type=ACTIVE_EXPECTATIONS')
    print(("Active api's:", json.dumps(res.json(), indent=4, sort_keys=True)))


if __name__ == "__main__":
   client = docker.from_env()
   operation = sys.argv[1]
   if operation == 'start':
    start_image(client, 'jamesdbloom/mockserver:latest')
   elif operation == 'stop':
    stop_containar(client, 'jamesdbloom/mockserver:latest')
   elif operation == 'setup':
       setup()
   elif operation == 'reset':
       reset()