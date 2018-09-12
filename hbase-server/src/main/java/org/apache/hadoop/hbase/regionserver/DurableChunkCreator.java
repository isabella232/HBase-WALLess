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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;
import org.apache.mnemonic.Reclaim;
import org.apache.mnemonic.Utils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DurableChunkCreator extends ChunkCreator {

  private DurableChunk<NonVolatileMemAllocator> durableBigChunk[];
  // Offset to track the allocation inside the bigChunk.
  private AtomicLong offset = new AtomicLong(0);
  // if we have a bigger value this does not work. So creating some random value for now
  // as in mnemonic's ChunkBufferNGTest
  // Test this new unique Ids concepts.
  private NonVolatileMemAllocator allocator;
  private DurableChunkRetrieverV2 retriever = null;
  private long memstoreSizeFactor;
  private int curChunk = 0;

  DurableChunkCreator(Configuration config, int chunkSize, long globalMemStoreSize, String durablePath) {
    super(chunkSize, true, globalMemStoreSize, 1.0F, 1.0F, null, 0);// TODO what should be last arg?
    // This config is used for easy testing. Once we get a sweet spot we can remove this - I
    // believe, if at all we get one
    int memstoreSize = config.getInt("hbase.memstoresize.factor", 8);
    // Do validation. but for now creating max sized allocator
    allocator = new NonVolatileMemAllocator(
      // creating twice the size of the configured memory. This works for now
        Utils.getNonVolatileMemoryAllocatorService("pmem"),
        (long) ((2 * globalMemStoreSize)), // TODO this 2x is not needed. Give correct value.
        durablePath, true);
    // TODO : Understand what is this
    allocator.setChunkReclaimer(new Reclaim<Long>() {
      @Override
      public boolean reclaim(Long mres, Long sz) {
        return false;
      }
    });
    // This does not work with > 15G
    // so let us create multiple smaller chunks based on the memstoreSizeFactor
    memstoreSizeFactor = memstoreSize * 1024l * 1024l * 1024l;
    int numOfChunks = (int)(globalMemStoreSize/memstoreSizeFactor);
    long remainingChunksLen = (globalMemStoreSize%memstoreSizeFactor);
    LOG.info("The memstoreSizeFactor is " + memstoreSizeFactor + " the numOfchunks " + numOfChunks
        + " remaining " + remainingChunksLen);
   // create as many chunks needed.
    durableBigChunk =
        remainingChunksLen > 0 ? new DurableChunk[numOfChunks + 1] : new DurableChunk[numOfChunks];
    long now = System.currentTimeMillis();
    // For now starting with some uniqueId - going with 23l because that is what we used previously
    long uniqueId = 23l;
    // On testing, with a memstore global size as around 33G we get around 4 chunks each of size 8G.
    // It takes around 112 secs on startup. We should find a sweet spot if at all it exists 
    for (int i = 0; i < numOfChunks; i++) {
      durableBigChunk[i] = allocator.createChunk(memstoreSizeFactor);
      if (durableBigChunk[i] == null) {
        throw new RuntimeException("Not able to create a durable chunk");
      }
      // TODO : See if we are able to retrieve back all the chunks with the uniqueId
      // Internally the native code does this
      //   if (key < MAX_HANDLER_STORE_LEN && key >= 0) {
      //    D_RW(root)->hdl_buf[key] = value; // -> see the array here. so this should work ideally
     //    }
      allocator.setHandler(uniqueId++, durableBigChunk[i].getHandler());
    }
    LOG.info("the time taken to create " + numOfChunks + " chunks is "
        + (System.currentTimeMillis() - now));
    now = System.currentTimeMillis();
    if (remainingChunksLen > 0) {
      durableBigChunk[durableBigChunk.length - 1] = allocator.createChunk(remainingChunksLen);
      if (durableBigChunk[durableBigChunk.length - 1] == null) {
        throw new RuntimeException("Not able to create that extra chunk durable chunk");
      }
      allocator.setHandler(uniqueId++, durableBigChunk[durableBigChunk.length - 1].getHandler());
    }
    LOG.info("the time taken to create the last chunk " + " chunks is "
        + (System.currentTimeMillis() - now));
  }

  @Override
  protected void initializePools(int chunkSize, long globalMemStoreSize, float poolSizePercentage,
      float indexChunkSizePercentage, float initialCountPercentage, HRegionServer hrs) {
    // TODO we need to deal with index chunks and pool also.
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
    for (; curChunk < durableBigChunk.length;) {
      DurableChunk<NonVolatileMemAllocator> tempChunk = this.durableBigChunk[curChunk];
      if ((long) (offset.get() + size) > this.memstoreSizeFactor) {
        // once we reach the max of one durable chunk, go to the next durable chunk and allocate
        // buffers out of it. note that once agian the offset will start from 0.
        this.offset.set(0);
        curChunk++;
        continue;
      }
      long offsetToUse = this.offset.getAndAdd(size);
      DurableSlicedChunk chunk = new DurableSlicedChunk(id, tempChunk, offsetToUse, size);
      addToChunkMap(chunk);
      return chunk;
    }
    // Ideally this should never happen
    LOG.error("there is an issue. Should never happen ");
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
    return new DurableMemStoreChunkPool(label, chunkSize, maxCount, initialCount,
        poolSizePercentage);
  }

  MemStoreChunkPool getDataPool() {
    return this.dataChunksPool;
  }

  MemStoreChunkPool getIndexPool() {
    return this.indexChunksPool;
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
        // retriver will be null here.
        if (ownerRegionStore == null || !(retriever.appendChunk(ownerRegionStore, chunk))) {
          // prepopulation to be done here.
          chunk.prepopulateChunk();
          reclaimedChunks.add(chunk);
        }
      }
    }
  }
}
