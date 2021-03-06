/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.blobit.pravega;

import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageFactoryInfo;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Preconditions;

/**
 * BlobIt {@link StorageFactoryCreator}
 * 
 * @author eolivelli
 * @author diego.salvi
 */
public class BlobItStorageFactoryCreator implements StorageFactoryCreator {

    @Override
    public StorageFactory createFactory(StorageFactoryInfo storageFactoryInfo, ConfigSetup setup, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(storageFactoryInfo, "storageFactoryInfo");
        Preconditions.checkNotNull(setup, "setup");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkArgument(storageFactoryInfo.getName().equals("BLOBIT"));
        return new BlobItStorageFactory(setup.getConfig(ChunkedSegmentStorageConfig::builder),
        		setup.getConfig(BlobItStorageConfig::builder), executor);
    }

    @Override
    public StorageFactoryInfo[] getStorageFactories() {
        return new StorageFactoryInfo[]{
            StorageFactoryInfo.builder()
                    .name("BLOBIT")
                    .storageLayoutType(StorageLayoutType.CHUNKED_STORAGE)
                    .build()
        };
    }
}
