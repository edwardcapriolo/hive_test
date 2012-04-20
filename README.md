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
