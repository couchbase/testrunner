
if [ $# != 1 ]; then
    echo "usage $0 <celery-stats.log>"
    exit -1
fi

stats="rddsk disk_util disk_wq disk_used swap usr_cpu rsize sys_cpu vsize wrdsk bg_fetched bg_fetch_wait"
file=$1

echo "Max"
echo "===="
for stat in $stats; do
    max=`cat $file  | grep $stat | awk '{print $5}' | sort -n | tail -1`
    host=`grep $max $file -B 15 | grep NODE | tail -1 | sed 's/.*NODE/NODE/g'`
    phase=`grep $max $file -B 15 | grep Phase | tail -1`
    echo $stat" : "$max" : "$host" : "$phase
done

echo -e "\n\n"
echo "Mean"
echo "===="
for stat in $stats; do
    mean=`cat $file  | grep $stat | awk '{ total += $11; count++ } END { print total/count }'`
    echo $stat" : "$mean
done

