Hive_test
=============

A simple way to test Hadoop and Hive using maven.

Setup with Maven
-----

By default, we're set to download a local copy of Hadoop when you first build Hive Test, or whenever the project is cleaned, just before running our test cases.

You can force a redownload and installation of Hadoop by manually activating the download-hadoop profile

    mvn --activate-profiles download-hadoop test

You can also perform the download and extraction process independent of testing.

Download Hadoop (into the maven target directory)

    mvn --activate-profiles download-hadoop wagon:download-single

Extract Hadoop  (into the maven target directory)

    mvn --activate-profiles download-hadoop exec:exec

Alternative
-----

We'll skip attempting to download and use a local copy of Hadoop if any of the following are true

* set your HADOOP_HOME environment variable to a hadoop distribution
* hadoop tar extracted to  $home/hadoop/hadoop-0.20.2_local

Hive Test will work so long as you have Hadoop 0.20.X in your path, i.e. /usr/bin/hadoop. In this case, you'll want to deactivate the hadoop download.

    mvn --activate-profiles -hadoop-home-defined test

Usage
-----

Hive_test gives us an embedded Hive including an embedded Derby database, 
and a local HiveThriftService. This allows us to create unit tests very easily.

    public class ServiceExampleTest extends HiveTestService {

      public ServiceExampleTest() throws IOException {
        super();
      }

      public void testExecute() throws Exception {
        Path p = new Path(this.ROOT_DIR, "afile");

        FSDataOutputStream o = this.getFileSystem().create(p);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
        bw.write("1\n");
        bw.write("2\n");
        bw.close();

        client.execute("create table  atest  (num int)");
        client.execute("load data local inpath '" + p.toString() 
          + "' into table atest");
        client.execute("select count(1) as cnt from atest");
        String row = client.fetchOne();
        assertEquals(row, "2");
        client.execute("drop table atest");
      }
    }

Builders
======

Nice for asserts! Sexy API, win!

    assertEquals(new ResultSet()
            .withRow(new Row().withColumn("2")).build(), client.fetchAll());
