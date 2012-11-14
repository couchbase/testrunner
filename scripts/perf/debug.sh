if [ "x$1" != "x" ] ; then
    echo "Collecting btree stats..."
    easy_install -U btrc > /dev/null
    hostname=`awk -F: '/\[servers\]/ { getline; print $0 }' $1 | cut -c 3-`
    btrc -n ${hostname}:8091

    echo "Collecting server info..."
    python scripts/collect_server_info.py -i $1 > /dev/null

    echo "Collecting atop stats..."
    python -m scripts.perf.grab_atops -i $1
fi