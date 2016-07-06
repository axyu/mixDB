#!/bin/bash

for((i=0;i<=112;i=i+4))
do
	./demo_data 500 5 $i >> ~/out/out500_5_
done
