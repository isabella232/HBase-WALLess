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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;
import org.apache.mnemonic.Reclaim;
import org.apache.mnemonic.Utils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class DurableChunkCreator extends ChunkCreator {

  private static final Logger LOG = LoggerFactory.getLogger(DurableChunkCreator.class);
  // Size for one durable chunk been created over the pmem.
  static final String DURABLECHUNK_SIZE = "hbase.durablechunk.size";
  private static final long DEFAULT_DURABLECHUNK_SIZE = 8L * 1024 * 1024 * 1024;// 8 GB

  private DurableChunk<NonVolatileMemAllocator> durableBigChunk[];
  // Offset to track the allocation inside the bigChunk.
  private long offsetInCurChunk = 0;
  // if we have a bigger value this does not work. So creating some random value for now
  // as in mnemonic's ChunkBufferNGTest
  // Test this new unique Ids concepts.
  private NonVolatileMemAllocator allocator;
  private DurableChunkRetrieverV2 retriever = null;
  private long durableBigChunkSize;
  private int curChunkIndex = 0;

  DurableChunkCreator(Configuration config, int chunkSize, long globalMemStoreSize, String durablePath) {
    super(chunkSize, true, globalMemStoreSize, 1.0F, 1.0F, null, 0);// TODO what should be last arg?
    // This config is used for easy testing. Once we get a sweet spot we can remove this - I
    // believe, if at all we get one
    // Do validation. but for now creating max sized allocator
    boolean exists = new File(durablePath).exists();
    if (exists) {
      LOG.info("Durable memstore backed file found. Hence retrieving data from it");
    }
    long allocatorSize = (long) ((2 * globalMemStoreSize));
    if (exists) {
      // capacity is not used when we want to get back the data. only on creation it is needed.
      allocatorSize = 1l;
    }
    allocator = new NonVolatileMemAllocator(
        // creating twice the size of the configured memory. This works for now
        Utils.getNonVolatileMemoryAllocatorService("pmem"), allocatorSize, // TODO this 2x is not needed. Give correctvalue.
        durablePath, !exists);
    // TODO : Understand what is this
    allocator.setChunkReclaimer(new Reclaim<Long>() {
      @Override
      public boolean reclaim(Long mres, Long sz) {
        return false;
      }
    });
    // This does not work with > 15G
    // so let us create multiple smaller chunks based on the memstoreSizeFactor
    durableBigChunkSize = config.getLong(DURABLECHUNK_SIZE, DEFAULT_DURABLECHUNK_SIZE);
    int numOfChunks = (int) (globalMemStoreSize / durableBigChunkSize);
    long remainingChunksLen = (globalMemStoreSize % durableBigChunkSize);
    LOG.info("Size of a durable chunk : " + durableBigChunkSize + ", Full size chunks : "
        + numOfChunks + ", Remaining size : " + remainingChunksLen);
   // create as many chunks needed.
    durableBigChunk =
        remainingChunksLen > 0 ? new DurableChunk[numOfChunks + 1] : new DurableChunk[numOfChunks];
    long now = System.currentTimeMillis();
    // For now starting with some uniqueId - going with 23l because that is what we used previously
    long uniqueId = 23l;
    // On testing, with a memstore global size as around 33G we get around 4 chunks each of size 8G.
    // It takes around 112 secs on startup. We should find a sweet spot if at all it exists
    for (int i = 0; i < numOfChunks; i++) {
      if (exists) {
        // retrieve the last chunk. TODO : check if we need to set the handler again
        durableBigChunk[i] = allocator.retrieveChunk(allocator.getHandler(uniqueId++));
        if (durableBigChunk[i] == null) {
          LOG.warn("The chunk is null");
        }
      } else {
        durableBigChunk[i] = allocator.createChunk(durableBigChunkSize);
        if (durableBigChunk[i] == null) {
          throw new RuntimeException("Not able to create a durable chunk");
        }
        LOG.info("the size of the chunk is " + durableBigChunk[i].getSize());
        // TODO : See if we are able to retrieve back all the chunks with the uniqueId
        // Internally the native code does this
        // if (key < MAX_HANDLER_STORE_LEN && key >= 0) {
        // D_RW(root)->hdl_buf[key] = value; // -> see the array here. so this should work ideally
        // }
        allocator.setHandler(uniqueId++, durableBigChunk[i].getHandler());

      }
    }
    if (remainingChunksLen > 0) {
      if (exists) {
        // retrieve the last chunk. TODO : check if we need to set the handler again
        durableBigChunk[durableBigChunk.length - 1] =
            allocator.retrieveChunk(allocator.getHandler(uniqueId));
      } else {
        durableBigChunk[durableBigChunk.length - 1] = allocator.createChunk(remainingChunksLen);
        if (durableBigChunk[durableBigChunk.length - 1] == null) {
          throw new RuntimeException("Not able to create that extra chunk durable chunk");
        }
        allocator.setHandler(uniqueId, durableBigChunk[durableBigChunk.length - 1].getHandler());
      }
    }
    LOG.info("Time taken to create all chunks : " + (System.currentTimeMillis() - now) + "ms");
  }

  @Override
  protected void initializePools(int chunkSize, long globalMemStoreSize, float poolSizePercentage,
      float indexChunkSizePercentage, float initialCountPercentage, HRegionServer hrs) {
    retriever = DurableChunkRetrieverV2.init(hrs);
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
      DurableChunk<NonVolatileMemAllocator> tempChunk = this.durableBigChunk[curChunkIndex];
      // TODO we should align the memstoreSizeFactor and chunkSize. There should not be any memory
      // waste from a DurableChunk.
      if (offsetInCurChunk + size > tempChunk.getSize()) {
        // once we reach the max of one durable chunk, go to the next durable chunk and allocate
        // buffers out of it.
        curChunkIndex++;
        this.offsetInCurChunk = 0;
        continue;
      }
      DurableSlicedChunk chunk = new DurableSlicedChunk(id, tempChunk, this.offsetInCurChunk, size);
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
    if (this.allocator != null) {
      this.allocator.close();
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

  MemStoreChunkPool getDataPool() {
    return this.dataChunksPool;
  }

  class DurableMemStoreChunkPool extends MemStoreChunkPool {

    DurableMemStoreChunkPool(String label, int chunkSize, int maxCount, int initialCount,
        float poolSizePercentage) {
      super(label, chunkSize, maxCount, initialCount, poolSizePercentage);
    }

    @Override
    protected void createInitialChunks(int chunkSize, int initialCount) {
      for (int i = 0; i < initialCount; i++) {
        DurableSlicedChunk chunk = createChunk(true, CompactingMemStore.IndexType.ARRAY_MAP,
            chunkSize);
        chunk.init();
        Pair<byte[], byte[]> ownerRegionStore = chunk.getOwnerRegionStore();
        // Still I see that on RS coming back the namespace and meta are not coming back online. Need to fix or see if any config issue
        if (ownerRegionStore == null || !(retriever.appendChunk(ownerRegionStore, chunk))) {
          chunk.prepopulateChunk();
          reclaimedChunks.add(chunk);
        }
      }
    }
  }
}
