/*
Copyright 2011 Edward Capriolo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.jointhegrid.hive_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;

public abstract class HiveTestService extends HiveTestBase {

    protected String host;
    protected int port;
    protected TTransport transport;
    protected HiveInterface client;
    protected boolean standAloneServer = false;

    public HiveTestService() throws IOException {
        super();
        host = "localhost";
        port = 10000;
    }

    public void setUp() throws Exception {
        super.setUp();
        Path rootDir = getDir(ROOT_DIR);
        Configuration conf = createJobConf();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(rootDir, true);
        Path metastorePath = new Path("/tmp/metastore_db");
        fs.delete(metastorePath, true);
        Path warehouse = new Path("/tmp/warehouse");
        fs.delete(warehouse, true);
        fs.mkdirs(warehouse);

        if (standAloneServer) {
            try {
                transport = new TSocket(host, port);
                TProtocol protocol = new TBinaryProtocol(transport);
                client = new HiveClient(protocol);
                transport.open();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        } else {
            client = new HiveServer.HiveServerHandler();
        }
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if (standAloneServer) {
            try {
                // client.clean();//not in 0.7.X
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
            transport.close();
        }
    }
}
