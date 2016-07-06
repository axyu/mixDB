#!/bin/bash

for((run_time=1;run_time<=5;run_time++))
do
	for((j=0;j<=11;j++))
	do
		for((i=0;i<=112;i++))
		do
			./demo_data 500 ${run_time} $i >> ~/out/out500_${run_time}_${j}_
		done
	done
done

for((run_time=1;run_time<=5;run_time++))
do
	for((j=0;j<=11;j++))
	do
		for((i=0;i<=112;i++))
		do
			./demo_data 1000 ${run_time} $i >> ~/out/out1000_${run_time}_${j}_
		done
	done
done
