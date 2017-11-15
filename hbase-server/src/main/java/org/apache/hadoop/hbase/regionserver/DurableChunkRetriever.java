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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.mnemonic.ChunkBuffer;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;
import org.apache.mnemonic.Reclaim;
import org.apache.mnemonic.Utils;

@InterfaceAudience.Private
public class DurableChunkRetriever {
  private Map<String, List<Cell>> regionChunks = new ConcurrentHashMap<String, List<Cell>>();
  private static final Log LOG = LogFactory.getLog(DurableChunkRetriever.class);
  private String durablePath;
  private NonVolatileMemAllocator allocator;
  // Offset to track the allocation inside the bigChunk.
  private AtomicLong offset = new AtomicLong(0);
  private int maxCount;
  // if we have a bigger value this does not work. So creating some random value for now
  // as in mnemonic's ChunkBufferNGTest
  private long uniqueId = 23l;
  private int chunkSize;
  private static DurableChunkRetriever INSTANCE;

  public static void initialize(String path, int chunkSize, int maxCount) {
    DurableChunkRetriever retriever = INSTANCE;
    if (retriever != null) return;
    if (new File(path).exists()) {
      INSTANCE = new DurableChunkRetriever(path, chunkSize, maxCount);
    }
  }

  public static DurableChunkRetriever getInstance() {
    return INSTANCE;
  }

  DurableChunkRetriever(String path, int chunkSize, int maxCount) {
    this.durablePath = path;
    this.maxCount = maxCount;
    try {
      allocator = new NonVolatileMemAllocator(
          Utils.getNonVolatileMemoryAllocatorService("pmem"), 1l, durablePath, false);
      // TODO : Understand what is this
      allocator.setChunkReclaimer(new Reclaim<Long>() {
        @Override
        public boolean reclaim(Long mres, Long sz) {
          return false;
        }
      });
      // this.uniqueId = this.durablePath.hashCode();
      this.chunkSize = chunkSize;
      DurableCellChunkCodec codec = new DurableCellChunkCodec();
      // TODO : fetch this on RS start itself or after RS is initialized and before region opening
      // happens?
      retrieveDurableChunks(chunkSize, maxCount, codec);
    } catch (Throwable t) {
      throw new RuntimeException("Exception while retrieving the chunks", t);
    }
  }

  private void retrieveDurableChunks(int chunkSize, int maxCount, DurableCellChunkCodec codec) {
    if (allocator != null) {
      // retrieve the chunk
      long hanlderId = allocator.getHandler(uniqueId);
      DurableChunk<NonVolatileMemAllocator> durableChunk = allocator.retrieveChunk(hanlderId);
      int count = 1;
      if (durableChunk != null) {
        for (int i = 0; i < maxCount; i++) {
          long offsetToUse = this.offset.getAndAdd(chunkSize);
          ChunkBuffer chunkBuffer = durableChunk.getChunkBuffer(offsetToUse, chunkSize);
          int chunkId = chunkBuffer.get().getInt(0);
          byte inUse = chunkBuffer.get().get(Bytes.SIZEOF_INT);
          // the region name is after the chunkId and the byte representing if the chunk is in use
          // or not
          if (LOG.isDebugEnabled()) {
            LOG.debug("The chunk is in Use?? " + inUse);
          }
          if (inUse == (byte) 1) {
            int endPremable = ByteBufferUtils.toInt(chunkBuffer.get(),
              Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE);
            // get only those chunks that are in use
            int regionSize = chunkBuffer.get().getInt(Bytes.SIZEOF_BYTE + 2 * Bytes.SIZEOF_INT);
            String regionName = ByteBufferUtils.toStringBinary(chunkBuffer.get(),
              Bytes.SIZEOF_BYTE + (3 * Bytes.SIZEOF_INT), regionSize);
            List<Cell> chunkList = this.regionChunks.get(regionName);
            if (chunkList == null) {
              chunkList = new ArrayList<Cell>();
            }
            this.regionChunks.put(regionName, chunkList);
            int startOffset = codec.getChunkMetaDataSize(regionSize);
            List<Cell> cells = codec.decode(startOffset, chunkBuffer.get(), endPremable);
            chunkList.addAll(cells);
          }
        }
        // close the allocator then and there
        close();
      }
    }
  }

  public List<Cell> getRegionChunk(String regionName) {
    return this.regionChunks.get(regionName);
  }

  // Unused
  // TODO : The problem is we don't know when to clear it. Say for example we have a start up of an
  // RS but none of the regions are found to be similar as those in the chunk
  public void clear() {
    this.regionChunks.clear();
  }

  public void clearRegionChunk(String regionName) {
    this.regionChunks.remove(regionName);
  }

  public void close() {
    // this close should be done after after the data is retrieved back
    allocator.close();
  }

  public static DurableChunkRetriever resetInstance() {
    DurableChunkRetriever cur = INSTANCE;
    INSTANCE = null;
    return cur;
  }
}
