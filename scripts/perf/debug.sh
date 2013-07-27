if [ "x$1" != "x" ] ; then
    echo "Collecting server info..."
    python scripts/collect_server_info.py -i $1 > /dev/null

    echo "Collecting atop stats..."
    python -m scripts.perf.grab_atops -i $1

    echo "Couchbase server log..."
    hostname=`awk -F: '/\[servers\]/ { getline; print $0 }' $1 | cut -c 3-`
    username=`grep rest_username $1 | awk -F: '{print $2}'`
    password=`grep rest_password $1 | awk -F: '{print $2}'`
    curl -m 2 $username:$password@$hostname:8091/logs 2> /dev/null | python -mjson.tool
fi
