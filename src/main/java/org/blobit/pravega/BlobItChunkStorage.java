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

import herddb.jdbc.HerdDBEmbeddedDataSource;
import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import io.pravega.segmentstore.storage.chunklayer.InvalidOffsetException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.NamedObjectMetadata;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectManagerFactory;
import org.blobit.core.api.PutOptions;

/**
 * BlobIt based ChunkStorage implementation
 *
 * @author eolivelli
 */
public class BlobItChunkStorage extends BaseChunkStorage {

    private final ObjectManager objectManager;
    private final HerdDBEmbeddedDataSource datasource;
    private final BucketHandle bucket;

    private final boolean extensiveMetadataChecks = true;

    BlobItChunkStorage(BlobItStorageConfig config, Executor executor) throws ObjectManagerException {
        super(executor);
        // TODO: configuration
        Configuration configuration = new Configuration().setType(Configuration.TYPE_BOOKKEEPER)
                /*
                 * if we know for each chunk the scope or stream name (currently not possible by
                 * design, we could leverage partitioned metadata feature)
                 */
                .setUseTablespaces(false).setConcurrentWriters(10).setEmptyLedgerMinTtl(0).setReplicationFactor(1)
                // is there any way to use the BK coonfiguration for tier1 ?
                .setZookeeperUrl(config.getBkUri());
        // TODO: configuration
        Properties dsProperties = new Properties();
        datasource = new HerdDBEmbeddedDataSource(dsProperties);
        datasource.setUrl(config.getJdbcUrl());
        objectManager = ObjectManagerFactory.createObjectManager(configuration, datasource);

        // missing a BaseChunkStorage#start() method.
        // here we are creating a java object and then performing
        // lots of expensive operations (booting the database and the ObjectManager)
        // handling a failure in the constructor is error prone
        try {
            objectManager.start();

            // this is another metadata operation, not to be issued inside this constructor
            if (objectManager.getBucketMetadata(config.getBucket()) == null) {
                objectManager.createBucket(config.getBucket(), config.getBucket(), BucketConfiguration.DEFAULT);
            }
            bucket = objectManager.getBucket(config.getBucket());
        } catch (ObjectManagerException err) {
            objectManager.close();
            throw err;
        }
    }

    @Override
    public void close() {
        objectManager.close();
        datasource.close();
        super.close();
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }

    @Override
    public boolean supportsAppend() {
        return true;
    }

    @Override
    public boolean supportsConcat() {
        return true;
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        NamedObjectMetadata metadata = getMetadata(chunkName);
        if (metadata == null) {
            throw new ChunkNotFoundException(chunkName, "BlobItChunkStorage::doGetInfo");
        }
        return ChunkInfo.builder().name(chunkName).length(metadata.getSize()).build();
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException {
        // NOOP ?
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        // TODO: If doCreate is a NOOP this could be an issue
        return getMetadata(chunkName) != null;
    }

    private NamedObjectMetadata getMetadata(String chunkName) throws ChunkStorageException {
        try {
            return bucket.statByName(chunkName);
        } catch (ObjectManagerException ex) {
            throw new ChunkStorageException(chunkName,
                    String.format("Error getting chunk %s metadata.", chunkName), ex);
        }
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException {
        // NOOP ?
        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException {
        // NOOP ?
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        /*
         * TODO: We could leverage async nature of the storage but at now we cannot
         * override doReadAsync due to unaccessible OperationContext
         */
        if (extensiveMetadataChecks) {
            if (!checkExists(handle.getChunkName())) {
                throw new ChunkNotFoundException(handle.getChunkName(), "BlobItChunkStorage::doDelete");
            }
        }
        try {
            bucket.deleteByName(handle.getChunkName()).get();
        } catch (ObjectManagerException ex) {
            throw new ChunkStorageException(handle.getChunkName(),
                    String.format("Error deleting chunk %s.", handle.getChunkName()), ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ChunkStorageException(handle.getChunkName(),
                    String.format("Interrupted while waiting for chunk %s deletion.", handle.getChunkName()), ex);
        }
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException {
        /*
         * TODO: We could leverage async nature of the storage but at now we cannot
         * override doReadAsync due to unaccessible OperationContext
         */
        AtomicLong read = new AtomicLong();
        try {
            bucket.downloadByName(handle.getChunkName(),
                    (numBytes) -> {
                        read.set(numBytes);
                    },
                    new BufferOutputStream(buffer, bufferOffset),
                    (int) fromOffset,
                    // this is a problem in BlobIt API, it should be fixed up there
                    length).get();
        } catch (ObjectManagerException ex) {
            throw new ChunkStorageException(handle.getChunkName(),
                    String.format("Error reading chunk %s.", handle.getChunkName()), ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ChunkStorageException(handle.getChunkName(),
                    String.format("Interrupted while waiting for chunk %s read.", handle.getChunkName()), ex);
        }
        return read.intValue();
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        /*
         * TODO: We could leverage async nature of the storage but at now we cannot
         * override doReadAsync due to unaccessible OperationContext
         */
        if (extensiveMetadataChecks) {
            NamedObjectMetadata metadata = getMetadata(handle.getChunkName());
            if (metadata.getSize() != offset) {
                throw new InvalidOffsetException(handle.getChunkName(), metadata.getSize(), offset, "doWrite");
            }
        }
        try {
            /*
             * It is expected to be offset == current size, in order to check this at this
             * level we should perform an additional metadata operation
             */
            bucket.put(handle.getChunkName(), length, data, PutOptions.APPEND).get();
            return length;
        } catch (ObjectManagerException ex) {
            throw new ChunkStorageException(handle.getChunkName(),
                    String.format("Error writing chunk %s.", handle.getChunkName()), ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ChunkStorageException(handle.getChunkName(),
                    String.format("Interrupted while writing for chunk %s read.", handle.getChunkName()), ex);
        }
    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        String first = null;
        int total = 0;
        for (ConcatArgument concat : chunks) {
            if (first == null) {
                first = concat.getName();
            } else {
                try {
                    bucket.concat(first, concat.getName());
                } catch (ObjectManagerException ex) {
                    throw new ChunkStorageException(concat.getName(),
                            String.format("Error during concat chunk %s with %s.", first, concat.getName()), ex);
                }
            }
            // if we are concatenating "long" lengths, why the return type is "int" ?
            // can we assume the "length" here is the size of the whole chunk ?
            total += (int) concat.getLength();
        }
        return total;
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly)
            throws ChunkStorageException, UnsupportedOperationException {
        // not supported, what happens if we do not support this feature ?
        // shall we have a isSetReadOnlySupported ?
    }

    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset)
            throws ChunkStorageException, UnsupportedOperationException {
        // this feature could be implemented in BlobIt, but now we could only
        // truncate chunks that are the result of "concat" operations
        // will it make sense to implement it ?
        return false;
    }

    private static class BufferOutputStream extends OutputStream {

        private final byte[] buffer;
        private int pos;

        public BufferOutputStream(byte[] buffer, int pos) {
            this.buffer = buffer;
            this.pos = pos;
        }

        @Override
        public void write(int b) throws IOException {
            buffer[pos++] = (byte) b;
        }

    }

}
