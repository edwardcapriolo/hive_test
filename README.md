hive_test
=============
A simple way to test Hive using Maven.

h3. Usage
-----

hive_test gives us an embedded Hive including an embedded Derby database,
and a local HiveThriftService. This allows us to create unit tests very easily.

Hive scripts testing can be done in a similar way to PigUnit:

        Map<String, String> params = Maps.newHashMap();
        File file = new File("src/test/resources/files/squidlog-SMALL.txt");

        params.put("$INPUT1", file.getAbsolutePath());
        Map<String, List<String>> input = Maps.<String, List<String>>newHashMap();
        List<String> lines = Lists.newArrayList();
        lines.add("1374100685.556 4 192.168.5.178 TCP_CLIENT_REFRESH_MISS/200 559 GET http://www.maps.google.com/8314c8d5-7df9-4abe-8acb-9366b7c887c2 mrodriguez DIRECT/192.168.5.117 text/html");
        lines.add("1374100685.556 4 192.168.5.178 TCP_CLIENT_REFRESH_MISS/200 619 POST http://www.facebook.com/d79fdfb5-c893-4dbc-bcf8-b670ebb21c35 mestevez DIRECT/192.168.5.117 text/html");
        lines.add("1374100685.556 4 192.168.5.178 TCP_CLIENT_REFRESH_MISS/200 957 POST http://www.lanacion.com.ar/f751d493-5620-4129-8ceb-07c5fa7feed7 mdominguez DIRECT/192.168.5.117 text/html");
        lines.add("1374100685.556 4 192.168.5.178 TCP_CLIENT_REFRESH_MISS/200 963 GET http://www.wikipedia.org/e27a4c3a-30dc-4c91-9cd3-2435a7d55eee mdominguez DIRECT/192.168.5.117 text/html");
        lines.add("1374100685.556 4 192.168.5.178 TCP_CLIENT_REFRESH_MISS/200 1368 GET http://www.google.com/analytics/f29a3c51-f0b8-4ce0-9592-05d12702de1f mrodriguez DIRECT/192.168.5.117 text/html");
        lines.add("1374100685.556 4 192.168.5.178 TCP_CLIENT_REFRESH_MISS/200 1069 POST http://www.google.com/analytics/6b8f7d8a-5832-439b-8dcf-34f60108b3d5 mestevez DIRECT/192.168.5.117 text/html");
        lines.add("1374100685.556 4 192.168.5.178 TCP_CLIENT_REFRESH_MISS/200 1315 GET http://www.twitter.com/30af5bdb-fa3e-4a8f-bcb1-bce6479dc413 mrodriguez DIRECT/192.168.5.117 text/html");
        lines.add("1374100685.556 4 192.168.5.178 TCP_CLIENT_REFRESH_MISS/200 1318 POST http://www.google.com/analytics/f03c65be-459e-4358-8653-0fa0e971cfb6 mestevez DIRECT/192.168.5.117 text/html");
        input.put("$INPUT1", lines);

        Response output =
                new HiveTest(TestingUtil.getDefaultProperties(),
                        "src/test/resources/scripts/squid-logs.hql", params)
                        .outputForInput(input);

        List<String> expected = Lists.newArrayList();
        expected.add("http://www.maps.google.com/8314c8d5-7df9-4abe-8acb-9366b7c887c2\tGET\t1.374100685556E9");

        assertEquals(expected, output.getResult());


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

h3. Using hive_test
h4. Setup with Maven

By default, we're set to download a local copy of Hadoop when you first build Hive Test, or whenever the project is cleaned, just before running our test cases.

You can force a redownload and installation of Hadoop by manually activating the download-hadoop profile

    mvn --activate-profiles download-hadoop test

You can also perform the download and extraction process independent of testing.

Download Hadoop (into the maven target directory)

    mvn --activate-profiles download-hadoop wagon:download-single

Extract Hadoop  (into the maven target directory)

    mvn --activate-profiles download-hadoop exec:exec


h4. Alternative

We'll skip attempting to download and use a local copy of Hadoop if any of the following are true

* set your HADOOP_HOME environment variable to a hadoop distribution
* hadoop tar extracted to  $home/hadoop/hadoop-0.20.2_local

Hive Test will work so long as you have Hadoop 0.20.X or  1.2.1 in your path, i.e. /usr/bin/hadoop. In this case, you'll want to deactivate the hadoop download.

    mvn --activate-profiles -download-hadoop test

h3. Development - setting up the project
When compiling the project, you may see an error regarding the jdo2-api:2.3-ec dependency.
Check [this link](https://issues.apache.org/jira/browse/HIVE-4114) to fix it.