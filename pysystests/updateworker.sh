ps aux | grep [c]elery | awk '{print $2}' | xargs kill  9
cp testcfg.py /tmp
sudo rm -rf /usr/local/lib/python2.6/dist-packages/couchbase*
cd /tmp
sudo rm -rf couchbase-python*
git clone https://github.com/couchbase/couchbase-python-client.git
cd couchbase-python-client
sudo python setup.py install
cd ~/
rm -rf testrunner
git clone http://github.com/couchbase/testrunner.git
cd testrunner/pysystests
cp /tmp/testcfg.py oldtestcfg.py
newparams=`cat testcfg.py  | egrep '^[A-Z].*=' | awk '{print $1}'`
oldparams=`cat oldtestcfg.py  | egrep '^[A-Z].*=' | awk '{print $1}'`

for param in $newparams; do
    inold=`echo $oldparams | grep "$param"`
    if [ -z "$inold" ]; then
       # add new param to restored cfg
       echo "Adding new param: ".$param
       newparam=`cat testcfg.py | egrep "^$param.*="`
       echo -e "\n$newparam" >> oldtestcfg.py
    fi
done

mv oldtestcfg.py testcfg.py
cat testcfg.py

echo "Done!"
echo "Make sure your cfg is correct!"
echo "To start worker: celery worker -A app -l ERROR -B --purge -c 16 -I app.init"
