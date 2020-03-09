#!/bin/bash

# Be sure you have maven installed before running this script.
# On linux: sudo apt install maven
# You'll need to set your JAVA_HOME properly.

cd ../LucenePartA
echo "Building Project Files"
sleep 2
mvn package

echo ""
echo "Building Lucene Index"
sleep 4
mvn exec:java -Dexec.mainClass="edu.ucr.cs.cs242.group14.LucenePartA" -Dexec.args="../sample_index ../sample_data \"marvel villans\" 1"
