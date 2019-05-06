/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lib.llpl.PersistentHeap;
import lib.llpl.PersistentMemoryBlock;

@InterfaceAudience.Private
public class DurableChunkCreator extends ChunkCreator {

  private static final Logger LOG = LoggerFactory.getLogger(DurableChunkCreator.class);
  private static final long DEFAULT_META_BLOCK_SIZE = 1024l;
  // Size for one durable chunk been created over the pmem.
  static final String DURABLECHUNK_SIZE = "hbase.durablechunk.size";
  // TODO : Just create 2MB memory blocks and also 2 MB bytebuffers. To see if that is making
  // the startup faster
  private static final long DEFAULT_DURABLECHUNK_SIZE = 8L * 1024 * 1024 * 1024;// 8 GB
  private String path = null;
  private PersistentMemoryBlock metaBlock;
  private PersistentMemoryBlock durableBigChunk[];
  private DurableChunkRetriever retriever = null;
  // Offset to track the allocation inside the bigChunk.
  private long offsetInCurChunk = 0;
  // if we have a bigger value this does not work. So creating some random value for now
  // as in mnemonic's ChunkBufferNGTest
  // Test this new unique Ids concepts.
  private PersistentHeap heap;
  private long durableBigChunkSize;
  private int curChunkIndex = 0;

  DurableChunkCreator(Configuration config, int chunkSize, long globalMemStoreSize,
      String durablePath) {
    super(chunkSize, true, globalMemStoreSize, 1.0F, 1.0F, null, 0);// TODO what should be last arg?
    this.path = durablePath;
    boolean exists = true;
    // TODO : LLPL needs to fix this for now passing 0 as suggested.
    // Also we need minimum 100G heap name spaces in devdax mode.
    // that is wasting too much of space. LLPL team has raised a concern regarding the same
    heap = PersistentHeap.getHeap(durablePath, 0);
    try {
    metaBlock = heap.memoryBlockFromHandle(heap.getRoot());
    } catch(Exception e ) {
      // the only way for now to know if it is a fresh start or a restart case.
      LOG.info("Probably a fresh start " , e);
      exists = false;
    }
    if (exists) {
      // capacity is not used when we want to get back the data. only on creation it is needed.
      LOG.info(
        "LLPL Durable memstore backed file found. Hence retrieving data from it " + durablePath);
      // we already have the meta block with us
    } else {
      // get the actual size that is required
      metaBlock = heap.allocateMemoryBlock(DEFAULT_META_BLOCK_SIZE, false);
      // set the meta block as the root for the heap. From the meta block retrieve other block
      // offsets
      heap.setRoot(metaBlock.handle());
    }
    // This does not work with > 15G
    // so let us create multiple smaller chunks based on the memstoreSizeFactor
    durableBigChunkSize = config.getLong(DURABLECHUNK_SIZE, DEFAULT_DURABLECHUNK_SIZE);
    int numOfChunks = (int) (globalMemStoreSize / durableBigChunkSize);
    long remainingChunksLen = (globalMemStoreSize % durableBigChunkSize);
    LOG.info("Size of a durable chunk : " + durableBigChunkSize + ", Full size chunks : "
        + numOfChunks + ", Remaining size : " + remainingChunksLen + " " + this.path);
    // create as many chunks needed.
    durableBigChunk = remainingChunksLen > 0 ? new PersistentMemoryBlock[numOfChunks + 1]
        : new PersistentMemoryBlock[numOfChunks];
    long now = System.currentTimeMillis();
    // On testing, with a memstore global size as around 33G we get around 4 chunks each of size 8G.
    // starting from offset 1 due to a bug in LLPL.
    long offset = 1l;
    for (int i = 0; i < numOfChunks; i++) {
      if (exists) {
        durableBigChunk[i] = heap.memoryBlockFromHandle(metaBlock.getLong(offset));
        if (durableBigChunk[i] == null) {
          LOG.warn("The chunk is null");
        }
        LOG.info("the size of the retrieved chunk is " + durableBigChunk[i].size());
      } else {
        durableBigChunk[i] = heap.allocateMemoryBlock(durableBigChunkSize, false);
        if (durableBigChunk[i] == null) {
          throw new RuntimeException("Not able to create a durable chunk");
        }
        LOG.info("the size of the chunk is " + durableBigChunk[i].size());
        metaBlock.setLong(offset, durableBigChunk[i].handle());
      }
      // increment the offset
      offset += Bytes.SIZEOF_LONG;
    }
    if (remainingChunksLen > 0) {
      if (exists) {
        // retrieve the last chunk
        durableBigChunk[durableBigChunk.length - 1] =
            heap.memoryBlockFromHandle(metaBlock.getLong(offset));
        LOG.info("the size of the retrieved chunk is " + durableBigChunk[durableBigChunk.length - 1] );
      } else {
        durableBigChunk[durableBigChunk.length - 1] =
            heap.allocateMemoryBlock(remainingChunksLen, false);
        if (durableBigChunk[durableBigChunk.length - 1] == null) {
          throw new RuntimeException("Not able to create that extra chunk durable chunk");
        }
        metaBlock.setLong(offset, durableBigChunk[durableBigChunk.length - 1].handle());
      }
    }
    LOG.info("Time taken to create all chunks : " + (System.currentTimeMillis() - now) + "ms");
  }

  @Override
  protected void initializePools(int chunkSize, long globalMemStoreSize, float poolSizePercentage,
      float indexChunkSizePercentage, float initialCountPercentage, HRegionServer hrs) {
    retriever = new DurableChunkRetriever(hrs);
    hrs.setRetriever(retriever);
    super.initializePools(chunkSize, globalMemStoreSize, poolSizePercentage,
      indexChunkSizePercentage, initialCountPercentage, hrs);
  }

  @Override
  protected DurableSlicedChunk createChunk(boolean pool,
      CompactingMemStore.IndexType chunkIndexType, int size) {
    if (!pool) {
      // For Durable chunks it must be pooled. Trying a random chunk here wont be work as that will
      // be on heap one and that will be volatile
      // TODO this exception is ok?
      throw new IllegalStateException();
    }
    int id = chunkID.getAndIncrement();

    assert id > 0;
    // We don't expect this to be called in a multi threaded way. For Durable Chunks, all the chunks
    // will be created at the starting of RS itself and will get pooled. We have check to make sure
    // this. So not handling the multi thread here intentionally.
    for (; curChunkIndex < durableBigChunk.length;) {
      PersistentMemoryBlock tempChunk = this.durableBigChunk[curChunkIndex];
      // TODO we should align the memstoreSizeFactor and chunkSize. There should not be any memory
      // waste from a DurableChunk.
      if (offsetInCurChunk + size > tempChunk.size()) {
        // once we reach the max of one durable chunk, go to the next durable chunk and allocate
        // buffers out of it.
        curChunkIndex++;
        this.offsetInCurChunk = 0;
        continue;
      }
      DurableSlicedChunk chunk =
          new DurableSlicedChunk(id, tempChunk, this.offsetInCurChunk, size, this);
      addToChunkMap(chunk);
      this.offsetInCurChunk += size;
      return chunk;
    }
    // Ideally this should never happen
    LOG.error("There is an issue. Should never happen!");
    return null;
  }

  // called during regionserver clean shutdown
  @Override
  protected void close() {
    // when there is abrupt shutdown and another process tries to read it we are able to
    // read the data. Even if the close has not happened
    if (this.heap != null) {
      this.heap.close();
    }
  }

  @Override
  protected MemStoreChunkPool createMemStoreChunkPool(String label, float poolSizePercentage,
      int chunkSize, int maxCount, int initialCount) {
    // TODO Index chunk pool need not be in Durable area. Handle. As of now we have hard coded the
    // index pool size % to be 0 so no impact.
    return new DurableMemStoreChunkPool(label, chunkSize, maxCount, initialCount,
        poolSizePercentage);
  }

  class DurableMemStoreChunkPool extends MemStoreChunkPool {

    DurableMemStoreChunkPool(String label, int chunkSize, int maxCount, int initialCount,
        float poolSizePercentage) {
      super(label, chunkSize, maxCount, initialCount, poolSizePercentage);
    }

    @Override
    protected void createInitialChunks(int chunkSize, int initialCount) {
      for (int i = 0; i < initialCount; i++) {
        DurableSlicedChunk chunk =
            createChunk(true, CompactingMemStore.IndexType.ARRAY_MAP, chunkSize);
        chunk.init();
        // we need to ensure we do this every time.
        Pair<byte[], byte[]> ownerRegionStore = chunk.getOwnerRegionStore();
        if (ownerRegionStore != null) {
          LOG.info("The region name is " + Bytes.toString(ownerRegionStore.getFirst()));
        }
        // Still I see that on RS coming back the namespace and meta are not coming back online.
        // Need to fix or see if any config issue
        if (ownerRegionStore == null || !(retriever.appendChunk(ownerRegionStore, chunk))) {
          // Not needed any more as the LLPL itself will do the prepopulate. So we should be good here
          //chunk.prepopulateChunk();
          reclaimedChunks.add(chunk);
        }
      }
    }
  }
}
