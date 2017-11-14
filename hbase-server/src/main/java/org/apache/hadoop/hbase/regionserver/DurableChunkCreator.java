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

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;
import org.apache.mnemonic.Reclaim;
import org.apache.mnemonic.Utils;

@InterfaceAudience.Private
public class DurableChunkCreator extends ChunkCreator {

  private static final Log LOG = LogFactory.getLog(DurableChunkCreator.class);

  private DurableChunk<NonVolatileMemAllocator> durableBigChunk;
  // Offset to track the allocation inside the bigChunk.
  private AtomicLong offset = new AtomicLong(0);
  // if we have a bigger value this does not work. So creating some random value for now
  // as in mnemonic's ChunkBufferNGTest
  private long uniqueId = 23l;
  private NonVolatileMemAllocator allocator;

  DurableChunkCreator(int chunkSize, long globalMemStoreSize, float poolSizePercentage,
      String durablePath) {
    super(chunkSize, true);
    // Do validation. but for now creating max sized allocator
    // As per Gary, pmalloc works with any size and pmem is not storage and space efficient
    allocator = new NonVolatileMemAllocator(
      // creating twice the size of the configured memory. This works for now
        Utils.getNonVolatileMemoryAllocatorService("pmem"),
        (long) ((2 * globalMemStoreSize * poolSizePercentage)),
        durablePath, true);
    // TODO : Understand what is this
    allocator.setChunkReclaimer(new Reclaim<Long>() {
      @Override
      public boolean reclaim(Long mres, Long sz) {
        return false;
      }
    });
    // This does not work with > 15G
    durableBigChunk = allocator.createChunk((long)((globalMemStoreSize * poolSizePercentage)));
    if (durableBigChunk == null) {
      throw new RuntimeException("Not able to create a durable chunk");
    }
    // this.uniqueId = durablePath.hashCode();
    // set the handler with the unique id
    allocator.setHandler(uniqueId, durableBigChunk.getHandler());
    long handler = allocator.getHandler(uniqueId);
  }

  protected void initializePool(long globalMemStoreSize, float poolSizePercentage,
      float initialCountPercentage, HeapMemoryManager heapMemoryManager) {
    if (poolSizePercentage != 1.0 || initialCountPercentage != 1.0) {
      // When Durable chunks in place, we need to have the entire global memstore size has to be
      // from pool.
      LOG.warn("When Durable chunks in place, we need to have the entire global memstore size has"
          + " to be from pool");
    }
    super.initializePool(globalMemStoreSize, 1.0F, 1.0F, heapMemoryManager);
    assert pool != null;
  }

  protected Chunk getChunk(String regionName) {
    // In case of DurableChunks, it has to come always from pool. Never create on demand. Return
    // null when no free chunk available. The null chunks to be handled down the line.
    Chunk chunk = null;
    chunk = this.pool.getChunk();
    if (chunk != null) {
      // put this chunk into the chunkIdMap
      this.chunkIdMap.put(chunk.getId(), new SoftReference<>(chunk));
    } else {
      if (LOG.isTraceEnabled()) {
        LOG.trace("The chunk pool is full. .");
        // TODO
      }
    }
    // do the init here. We reset the chunk in pool.getchunk and again init() it. already this would
    // have been inited
    // while chunk pool was created. Fix it in trunk also
    chunk.init(regionName);
    return chunk;
  }

  @Override
  protected Chunk createChunk(boolean pool) {
    assert pool;
    int id = chunkID.getAndIncrement();
    assert id > 0;
    long offsetToUse = this.offset.getAndAdd(chunkSize);
    return new DurableSlicedChunk(id, this.durableBigChunk, offsetToUse, chunkSize);
  }

  // called during regionserver clean shutdown
  protected void close() {
    // when there is abrupt shutdown and another process tries to read it we are able to
    // read the data. Even if the close has not happened
    if (this.allocator != null) {
      this.allocator.close();
    }
  }
}
