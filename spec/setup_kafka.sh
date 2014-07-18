#!/bin/sh

cd spec/

git clone https://github.com/apache/kafka.git
cd kafka
git checkout 0.8.1
./gradlew jar
