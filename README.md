## hive_test: A simple way to test Hive scripts.

[![Build Status](https://travis-ci.org/jmrozanec/hive_test.svg?branch=master)](https://travis-ci.org/jmrozanec/hive_test)

### Usage

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


### Maven

#### Available from Maven central

Declare the Maven dependency in your pom.xml file

        <dependencies>
            ...
                <dependency>
                    <groupId>io.teknek</groupId>
                    <artifactId>hiveunit</artifactId>
                    <version>0.0.3</version>
                    <classifier>jar-with-dependencies</classifier>
                </dependency>
             ...
        <dependencies>


#### Execution profiles

##### Without a local Hadoop install

By default, we're set to download a local copy of Hadoop when you first build Hive Test, or whenever the project is cleaned, just before running our test cases.

You can force a redownload and installation of Hadoop by manually activating the download-hadoop profile

    mvn --activate-profiles download-hadoop test

You can also perform the download and extraction process independent of testing.

Download Hadoop (into the maven target directory)

    mvn --activate-profiles download-hadoop wagon:download-single

Extract Hadoop  (into the maven target directory)

    mvn --activate-profiles download-hadoop exec:exec


##### With a local Hadoop install

We'll skip attempting to download and use a local copy of Hadoop if any of the following are true

* set your HADOOP_HOME environment variable to a hadoop distribution
* hadoop tar extracted to  $home/hadoop/hadoop-0.20.2_local

Hive Test will work so long as you have Hadoop in your path, i.e. /usr/bin/hadoop. In this case, you'll want to deactivate the hadoop download.

    mvn --activate-profiles -hadoop-home-defined test



### Contribute!

Contributions are welcome! You can contribute by
 * starring this repo!
 * adding new features
 * enhancing existing code
 * testing
 * enhancing documentation
 * bringing suggestions and reporting bugs
 * spreading the word / telling us how you use it!
