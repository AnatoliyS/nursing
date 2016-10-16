#!/bin/bash
cat all*.csv >data.csv
gawk 'BEGIN {srand()} {f = (rand() <= 0.8 ? "train.csv" : "test.csv"); print > f}' data.csv

