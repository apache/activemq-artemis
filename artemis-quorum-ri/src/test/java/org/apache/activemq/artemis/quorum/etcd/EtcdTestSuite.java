/*
 * Copyright 2017, 2018 IBM Corp. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.activemq.artemis.quorum.etcd;

import java.nio.file.Path;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
   EtcdDistributedLockTest.class,
   EtcdConfigTest.class
})
public class EtcdTestSuite {

    static Process etcdProcess, etcdTlsProcess, etcdTlsCaProcess;

    static final String etcdCommand;
    static {
         String etcd = System.getenv("ETCD_CMD");
         etcdCommand = etcd != null ? etcd : "etcd";
    }

    static final String clientKey = EtcdTestSuite.class.getResource("/client.key").getFile();
    static final String clientCert = EtcdTestSuite.class.getResource("/client.crt").getFile();
    static final String serverKey = EtcdTestSuite.class.getResource("/server.key").getFile();
    static final String serverCert = EtcdTestSuite.class.getResource("/server.crt").getFile();

    private static TemporaryFolder tmpFolder = new TemporaryFolder();

    private static Path etcdDataFolder1;
    private static Path etcdWalFolder1;
    private static Path etcdDataFolder2;
    private static Path etcdWalFolder2;
    private static Path etcdDataFolder3;
    private static Path etcdWalFolder3;

    @BeforeClass
    public static void setUp() throws Exception {
        tmpFolder.create();
        etcdDataFolder1 = EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "default-dat-dir");
        etcdWalFolder1 = EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "default-wal-dir");
        etcdDataFolder2 = EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "tls-dat-dir");
        etcdWalFolder2 = EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "tls-wal-dir");
        etcdDataFolder3 = EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "tls-ca-dat-dir");
        etcdWalFolder3 = EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "tls-ca-wal-dir");
        etcdProcess = EtcdProcessHelper.startProcess(
                "--data-dir=" + etcdDataFolder1.toString(),
                "--wal-dir=" + etcdWalFolder1.toString()
        );
        etcdTlsProcess = EtcdProcessHelper.startProcess("--cert-file=" + serverCert,
                "--key-file=" + serverKey, "--listen-client-urls=https://localhost:2360",
                "--listen-peer-urls=http://localhost:2361",
                "--advertise-client-urls=https://localhost:2360", "--name=tls",
                "--data-dir=" + etcdDataFolder2.toString(),
                "--wal-dir=" + etcdWalFolder2.toString()
        );
        etcdTlsCaProcess = EtcdProcessHelper.startProcess("--cert-file=" + serverCert,
                "--key-file=" + serverKey, "--listen-client-urls=https://localhost:2362",
                "--listen-peer-urls=http://localhost:2363",
                "--advertise-client-urls=https://localhost:2362", "--name=tls-ca", 
                "--trusted-ca-file=" + clientCert, "--client-cert-auth",
                "--data-dir=" + etcdDataFolder3.toString(),
                "--wal-dir=" + etcdWalFolder3.toString()
        );
    }

    @AfterClass
    public static void tearDown() {
        EtcdProcessHelper.tearDown(etcdProcess);
        EtcdProcessHelper.tearDown(etcdTlsProcess);
        EtcdProcessHelper.tearDown(etcdTlsCaProcess);
        EtcdProcessHelper.cleanup(tmpFolder);
    }

}
