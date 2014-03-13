hive_test
=============
A simple way to test Hive scripts using Maven.

# Usage
-----
### How to write a test using hive_test

hive_test gives us an embedded Hive including an embedded Derby database,
and a local HiveThriftService. This allows us to create unit tests very easily.

Hive scripts testing can be done in a similar way to [PigUnit](http://pig.apache.org/docs/r0.8.1/pigunit.html).
For more examples check [this class](https://github.com/jmrozanec/hive_test/blob/master/src/test/java/com/jointhegrid/hive_test/HiveTestTest.java)

        @Test
            public void testScriptListInput() {
                Map<String, List<String>> input = Maps.<String, List<String>>newHashMap();
                List<String> lines = Lists.newArrayList();
                lines.add("msmith,10");
                lines.add("mjohnson,2");
                lines.add("mwilliamson,7");
                lines.add("mjones,4");
                lines.add("mdavies,5");

                input.put("$INPUT1", lines);

                Response output =
                        HiveBuilder.create()
                                .hiveTestWithEmbeddedHive("src/test/resources/scripts/passing-scores.hql")
                                .outputForInput(input);

                List<String> expected = Lists.newArrayList();
                expected.add("msmith,10");
                expected.add("mwilliamson,7");

                assertEquals(ResponseStatus.SUCCESS, output.getResponseStatus());
                assertEquals(expected, output.getResult());
            }


There are still test classes extending from JUnit that can also be used for this, though we recommend the first approach:


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

-----

### Add Maven dependency

Declare the Maven dependency in your pom.xml file

        <dependencies>
            ...
                <dependency>
                    <groupId>com.jointhegrid</groupId>
                    <artifactId>hive_test</artifactId>
                    <version>4.3.0-SNAPSHOT</version>
                    <classifier>jar-with-dependencies</classifier>
                </dependency>
             ...
        <dependencies>


# Development

### Setting up the project

When compiling the project, you may see an error regarding the jdo2-api:2.3-ec dependency.
Check [this link](https://issues.apache.org/jira/browse/HIVE-4114) to fix it.

### Setup with Maven

By default, we're set to download a local copy of Hadoop when you first build Hive Test, or whenever the project is cleaned, just before running our test cases.

You can force a redownload and installation of Hadoop by manually activating the download-hadoop profile

    mvn --activate-profiles download-hadoop test

You can also perform the download and extraction process independent of testing.

Download Hadoop (into the maven target directory)

    mvn --activate-profiles download-hadoop wagon:download-single

Extract Hadoop  (into the maven target directory)

    mvn --activate-profiles download-hadoop exec:exec


### Alternative

We'll skip attempting to download and use a local copy of Hadoop if any of the following are true

* set your HADOOP_HOME environment variable to a hadoop distribution
* hadoop tar extracted to  $home/hadoop/hadoop-0.20.2_local

Hive Test will work so long as you have Hadoop 0.20.X or  1.2.1 in your path, i.e. /usr/bin/hadoop. In this case, you'll want to deactivate the hadoop download.

    mvn --activate-profiles -download-hadoop test