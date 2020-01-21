import sys

try:
    from pip import main as pipmain
except:
    from pip._internal import main as pipmain

def install(package):
     pipmain(['install', package])

# Example
if __name__ == '__main__':
    dep = sys.argv[1]
    install(dep)