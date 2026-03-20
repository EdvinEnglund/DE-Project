# DE-Project

1.start master VM
    instance snapshot: group16-master-snap
    volume size: 25Gb
    Flavor: ssc.medium
    Networks: bypass.net

2.start worker VM
    Count: 3
    instance snapshot: group16-worker-snap
    volume size: 25Gb
    Flavor: ssc.medium
    Networks: bypass.net

3.Associate floating IP to the master VM

4.SSH into the master VM
5.Run   $git clone https://github.com/EdvinEnglund/DE-Project.git
6.$cd DE-Project/scripts
7.$nano cluster.env
8.edit MASTER_HOST, MASTER_IP, WORKER_HOSTS and WORKER_IPS with the names and IPs of the new VMs
9.$bash bootstrap_cluster.sh
10. nano importTaxiFiles.py
11. edit TARGET_SIZE_GB to desired download size
12. edit HDFS_BIN and HDFS_DIR to desired HDFS dir
13. edit "year" and "min_year" to desired measure years.
14. python3 importTaxiFiles.py


