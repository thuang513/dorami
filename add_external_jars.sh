#!/bin/bash

# Add haloop libs
mvn install:install-file -DgroupId=org.apache.hadoop -DartifactId=HaLoop \
-Dversion=1.0 -Dpackaging=jar -Dfile=lib/hadoop-0.20.2-dev-core.jar