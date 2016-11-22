#!/usr/bin/env bash

sizes=( 1000, 2000, 4000, 8000, 16000, 32000, 64000 )
no_msgs=10000
amqp_url="amqp://d001:5672"
delay=10
results=rdma

for size in "${sizes[@]}"
do
   :
  java -cp ../../target/dsc-stream-performance-0.1-jar-with-dependencies.jar edu.indiana.soic.dsc.stream.perf.DataGenerator $amqp_url $results/$size $delay $size $no_msgs
done
