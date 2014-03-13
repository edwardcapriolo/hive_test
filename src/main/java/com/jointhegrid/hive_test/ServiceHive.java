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

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ServiceHive {

    public TTransport transport;
    public HiveInterface client;

    public ServiceHive() throws MetaException {
        client = new HiveServer.HiveServerHandler();
    }

    public ServiceHive(String host, int port) {
        transport = new TSocket(host, port);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new HiveClient(protocol);
        try {
            transport.open();
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    }
}
