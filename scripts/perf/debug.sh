if [ -z ${ini_file} ] ; then
    echo "Collecting btree stats..."
    easy_install btrc > /dev/null
    hostname=awk -F: '/\[servers\]/ { getline; print $0 }' ${ini_file} | cut -c 3-
    btrc -n ${hostname}:8091

    echo "Collecting server info..."
    python scripts/collect_server_info.py -i ${ini_file}
fi