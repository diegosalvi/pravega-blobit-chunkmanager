package org.blobit.pravega;

import io.pravega.local.LocalPravegaEmulator;
import org.junit.Test;

public class BlobItStorageIntegrationTest {
    
    static{
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
        System.setProperty("zookeeper.admin.enableServer", "false");
    }
    
    @Test
    public void test() throws Exception {
        int port = 23214;
        String url = "pravega://localhost:"+port;
        try (LocalPravegaEmulator emulator = LocalPravegaEmulator
                .builder()
                .enableRestServer(false)
                .zkPort(9897)
                .controllerPort(port)
                .segmentStorePort(8322)
                .build();) {
            emulator.start();
        }
    }
}
