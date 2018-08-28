#!/usr/bin/env bash

docker stop kafka

docker stop hadoop

docker stop zk


rm -rf /tmp/spark-test