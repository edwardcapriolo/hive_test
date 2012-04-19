Hive_test
=============

A simple way to test Hadoop and Hive using maven.

Setup with Maven
-----

Run these commands initially or whenever your project is cleaned.

Download Hadoop (into the maven target directory)

    mvn wagon:download-single

Extract Hadoop  (into the maven target directory)

    mvn exec:exec

Alternative
-----

* hadoop 0.20.X in your path ie /usr/bin/hadoop
* set your HADOOP_HOME environment variable to a hadoop distribution
* hadoop tar extracted to  $home/hadoop/hadoop-0.20.2-local

