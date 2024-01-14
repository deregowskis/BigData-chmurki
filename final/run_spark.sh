#!/bin/bash

counter=0

while true; do
    ((counter++))
    
    current_timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    output_log="spark_output_$counter.log"
    
    echo " "
    echo "Iteration ................. $counter"
    echo "Current timestamp: ........ $current_timestamp"
    echo "Log file: ................. $output_log"
    
    rm -f "$output_log" && echo "File $output_log ... removed successfully, or it didn't exist." 

    # run spark
    nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 main_stream.py > "$output_log" 2>&1 &
    pid=$!
    wait $pid
    
    #if feiled then restart
    if [ $? -eq 0 ]; then
        echo "Spark job (Attempt $counter) exited successfully. Output saved to $output_log"
    else
        echo "Spark job (Attempt $counter) failed. Restarting..."
    fi
    sleep 60
done