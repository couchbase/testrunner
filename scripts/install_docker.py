try:
    from pip import main as pipmain
except:
    from pip._internal import main as pipmain

def install(package):
     pipmain(['install', package])

# Example
if __name__ == '__main__':
    install('docker')