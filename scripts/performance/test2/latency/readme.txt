./heron submit slurm ~/projects/dsc-stream/performance/target/dsc-stream-performance-0.1-jar-with-dependencies.jar edu.indiana.soic.dsc.stream.perf.ChainTopology -name bcast_processor -p 8 -mode lf -stmgr 4


#!/usr/bin/env bash

#sizes=( 1000 2000 4000 1000 2000 4000 8000 16000 32000 64000 128000 256000 512000)
sizes=( 32000 32000 32000 16000 64000 128000 256000 512000 1024000 2048000)
#sizes=( 512000)
no_msgs=4000
amqp_url="amqp://d001:5672"
delay=25
results=test2/latency/tcp5

for size in "${sizes[@]}"
do
  :
 java -cp ../../performance/target/dsc-stream-performance-0.1-jar-with-dependencies.jar edu.indiana.soic.dsc.stream.perf.DataGenerator $amqp_url $results/$size $delay $size $no_msgs
 sleep 10
done

