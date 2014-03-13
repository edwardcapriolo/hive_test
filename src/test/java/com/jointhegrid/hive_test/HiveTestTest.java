package com.jointhegrid.hive_test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jointhegrid.hive_test.common.Response;
import com.jointhegrid.hive_test.dsl.HiveBuilder;
import com.jointhegrid.hive_test.dsl.HiveTest;
import com.jointhegrid.hive_test.util.TestingUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class HiveTestTest {

    private Properties properties;

    @Before
    public void setUp() {
        try {
            this.properties = new Properties();
            properties.load(new FileInputStream("testing.properties"));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The following test loads a script and tests execution against a given input file
     */
    @Test
    public void testScriptFileInput() {
        Map<String, File> inputFiles = Maps.newHashMap();
        File file = new File("src/test/resources/files/squidlog-SMALL.txt");
        inputFiles.put("$INPUT1", file);

        Response output =
                HiveBuilder.create()
                        .withClientProperties(TestingUtil.getDefaultProperties())
                        .hiveTestWithEmbeddedHive("src/test/resources/scripts/squid-logs.hql")
                        .outputForInputFiles(inputFiles);

        List<String> expected = Lists.newArrayList();
        expected.add("http://www.maps.google.com/8314c8d5-7df9-4abe-8acb-9366b7c887c2\tGET\t1.374100685556E9");

        assertEquals(expected, output.getResult());
    }

    @Test
    public void testScriptListInput() {
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
                HiveBuilder.create()
                        .withClientProperties(TestingUtil.getDefaultProperties())
                        .hiveTestWithEmbeddedHive("src/test/resources/scripts/squid-logs.hql")
                        .outputForInput(input);

        List<String> expected = Lists.newArrayList();
        expected.add("http://www.maps.google.com/8314c8d5-7df9-4abe-8acb-9366b7c887c2\tGET\t1.374100685556E9");

        assertEquals(expected, output.getResult());
    }
}
