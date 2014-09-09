## hive_test: A simple way to test Hive scripts.

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


### Available at Maven central!

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


### Contribute!

Contributions are welcome! You can contribute by
 * starring this repo!
 * adding new features
 * enhancing existing code: ex.: provide more accurate description cases
 * testing
 * enhancing documentation
 * bringing suggestions and reporting bugs
 * spreading the word / telling us how you use it!