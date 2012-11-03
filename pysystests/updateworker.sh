ps aux | grep [c]elery | awk '{print $2}' | xargs kill  9
cp testcfg.py /tmp
sudo rm -rf /usr/local/lib/python2.6/dist-packages/couchbase*
cd /tmp
rm -rf couchbase-python*
git clone https://github.com/tahmmee/couchbase-python-client.git
cd couchbase-python-client
sudo python setup.py install
cd ~/
rm -rf testrunner
git clone http://github.com/membase/testrunner.git
cd testrunner/pysystests
cp /tmp/testcfg.py .
cat testcfg.py

echo "Done!"
echo "Make sure your cfg is correct!"
echo "To start worker: celery worker -A app -l ERROR -B --purge -c 16 -I app.init"
