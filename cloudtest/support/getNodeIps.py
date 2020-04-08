import sys
from kubernetes import client, config

def main():
    currNameSpace=sys.argv[1]
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    nodeList = v1.list_pod_for_all_namespaces(watch=False)

    for node in nodeList.items:
        if node.metadata.namespace == currNameSpace:
            print(("%s %s" % (node.metadata.name, node.status.pod_ip)))

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide namespace to process")
        sys.exit(1)

    main()

