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

import com.google.common.base.Preconditions;	
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 *
 * @author eolivelli
 */
public class BlobItStorageConfig {
    
    public static final Property<String> BKCONFIGURI = Property.named("bk.config.uri", "lcoalhost:2181");
    public static final Property<String> JDBCCONFIGURI = Property.named("jdbc.config.uri", "jdbc:herddb:zookeeper:localhost:2181/herd");
    public static final Property<String> BUCKET = Property.named("bucket", "");
    
    @Getter
    private final String bkUri;

    @Getter
    private final String jdbcUrl;
    
    @Getter	
    private final String bucket;
    
    private BlobItStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.bucket = Preconditions.checkNotNull(properties.get(BUCKET), "bucket");
        this.bkUri = Preconditions.checkNotNull(properties.get(BKCONFIGURI), "bk.config.uri");
        this.jdbcUrl = Preconditions.checkNotNull(properties.get(JDBCCONFIGURI), "jdbc.config.uri");
        // TODO: plumb all configuration options
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<BlobItStorageConfig> builder() {
        return new ConfigBuilder<>("BLOBIT", BlobItStorageConfig::new);
    }

    //endregion
}
