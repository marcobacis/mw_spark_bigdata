#!/usr/bin/env bash

mkdir data
cd data

for i in 'seq 1994 2008'
do
    wget http://stat-computing.org/dataexpo/2009/$i.csv.bz2
done
