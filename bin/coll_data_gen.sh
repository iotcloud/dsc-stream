#!/usr/bin/env bash

sizes=( 32 32 32 16 32 64 128 256 512 1024 )
sizes=( 32 32 )
sizes=( 1000 1000 2000 4000 8000 16000 32000 )
#sizes=( 32000 32000 32000 16000 64000 )
sizes=(16000 16000 32000)
delays=(400 400 10)
no_msgs=4000
amqp_url="amqp://j-080:5672"
delay=400
#results=tcp_copy/latency4
results=col/16_128_reduce2

#for size in "${sizes[@]}"
for (( i = 0 ; i < ${#sizes[@]} ; i=$i+1 ));
do
  :
 size=${sizes[${i}]}
 delay=${delays[${i}]}
 java -Xms4G -Xmx4G -cp ../collectives/target/dsc-stream-collectives-0.1-jar-with-dependencies.jar edu.indiana.soic.dsc.stream.collectives.DataGenerator $amqp_url $results/$size $delay $size $no_msgs
 sleep 10
done
