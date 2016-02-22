#!/usr/bin/env bash
# This doesn't work properly, but got me to a different error message!
wget http://download.oracle.com/otn/utilities_drivers/jdbc/11203/ojdbc6_g.jar
mvn install:install-file -DgroupId=com.oracle -DartifactId=jdbc_11g  -Dversion=11.2.0.3.0 -Dpackaging=jar -Dfile=ojdbc6_g.jar -DgeneratePom=true
