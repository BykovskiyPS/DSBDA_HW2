#!/bin/bash
echo "Creating .jar file..."

mvn package -f ../pom.xml
cp ../target/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar /tmp
