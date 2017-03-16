#!/usr/bin/env bash

sizes=( 32 32 32 16 32 64 128 256 512 1024 )
sizes=( 32 32 )
sizes=( 32000 32000 32000 16000 64000 128000 256000 512000 1024000 )
#sizes=( 32000 32000 32000 16000 64000 )
#sizes=( 64000)
no_msgs=4000
amqp_url="amqp://j-080:5672"
delay=10
#results=tcp_copy/latency4
results=march17/verbs/latency

for size in "${sizes[@]}"
do
  :
 java -Xms4G -Xmx4G -cp ../../performance/target/dsc-stream-performance-0.1-jar-with-dependencies.jar edu.indiana.soic.dsc.stream.perf.DataGenerator $amqp_url $results/$size $delay $size $no_msgs
 sleep 10
done

