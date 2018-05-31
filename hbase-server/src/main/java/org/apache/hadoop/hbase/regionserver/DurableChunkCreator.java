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

import org.apache.hadoop.hbase.util.Pair;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;
import org.apache.mnemonic.Reclaim;
import org.apache.mnemonic.Utils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DurableChunkCreator extends ChunkCreator {

  private DurableChunk<NonVolatileMemAllocator> durableBigChunk;
  // Offset to track the allocation inside the bigChunk.
  private AtomicLong offset = new AtomicLong(0);
  // if we have a bigger value this does not work. So creating some random value for now
  // as in mnemonic's ChunkBufferNGTest
  private long uniqueId = 23l;
  private NonVolatileMemAllocator allocator;
  private DurableChunkRetrieverV2 retriever = null;

  DurableChunkCreator(int chunkSize, long globalMemStoreSize, String durablePath) {
    super(chunkSize, true, globalMemStoreSize, 1.0F, 1.0F, null, 0);// TODO what should be last arg?
    // Do validation. but for now creating max sized allocator
    // As per Gary, pmalloc works with any size and pmem is not storage and space efficient
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
    durableBigChunk = allocator.createChunk((long)((globalMemStoreSize)));
    if (durableBigChunk == null) {
      throw new RuntimeException("Not able to create a durable chunk");
    }
    // this.uniqueId = durablePath.hashCode();
    // set the handler with the unique id
    // TODO what is this used for?
    allocator.setHandler(uniqueId, durableBigChunk.getHandler());
  }

  @Override
  protected void initializePools(int chunkSize, long globalMemStoreSize, float poolSizePercentage,
      float indexChunkSizePercentage, float initialCountPercentage,
      HeapMemoryManager heapMemoryManager) {
    super.initializePools(chunkSize, globalMemStoreSize, poolSizePercentage,
        indexChunkSizePercentage, initialCountPercentage, heapMemoryManager);
    // TODO we need to deal with index chunks and pool also.
    retriever = DurableChunkRetrieverV2.init((DurableMemStoreChunkPool) this.dataChunksPool);
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
    long offsetToUse = this.offset.getAndAdd(size);
    DurableSlicedChunk chunk = new DurableSlicedChunk(id, this.durableBigChunk, offsetToUse, size);
    addToChunkMap(chunk);
    return chunk;
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
        if (ownerRegionStore == null || !(retriever.appendChunk(ownerRegionStore, chunk))) {
          reclaimedChunks.add(chunk);
        }
      }
    }
  }
}
