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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.mnemonic.ChunkBuffer;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;
import org.apache.mnemonic.Reclaim;
import org.apache.mnemonic.Utils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DurableChunkRetriever {
  private Map<Pair<String, String>, Map<Short, ByteBuffer>> regionFamilyChunks =
      new ConcurrentHashMap<Pair<String, String>, Map<Short, ByteBuffer>>();
  
  private Map<Pair<String, String>, List<Cell>> regionFamilyCells =
      new ConcurrentHashMap<Pair<String, String>, List<Cell>>();
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

  // TODO : How faster we  bring back the data ? Does it matter here??
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
      // TODO : fetch this on RS start itself or after RS is initialized and before region opening
      // happens?
      retrieveDurableChunks();
    } catch (Throwable t) {
      throw new RuntimeException("Exception while retrieving the chunks", t);
    }
  }

  private void retrieveDurableChunks() {
    if (allocator != null) {
      // retrieve the chunk
      long hanlderId = allocator.getHandler(uniqueId);
      DurableChunk<NonVolatileMemAllocator> durableChunk = allocator.retrieveChunk(hanlderId);
      if (durableChunk != null) {
        for (int i = 0; i < maxCount; i++) {
          long offsetToUse = this.offset.getAndAdd(chunkSize);
          ChunkBuffer chunkBuffer = durableChunk.getChunkBuffer(offsetToUse, chunkSize);
          if (chunkBuffer != null) {
            ByteBuffer buffer = chunkBuffer.get();
            int chunkId = buffer.getInt(0);
            int inUse = buffer.getInt(DurableSlicedChunk.OFFSET_TO_SEQID
                + DurableSlicedChunk.SIZE_OF_SEQID + Bytes.SIZEOF_INT);
            int offset = DurableSlicedChunk.OFFSET_TO_SEQID + DurableSlicedChunk.SIZE_OF_SEQID
                + DurableSlicedChunk.SIZE_OF_OFFSETMETA;
            if (inUse != 0) {
              int regionNameLength = buffer.getInt(offset);
              String regionName = ByteBufferUtils.toStringBinary(buffer, offset + Bytes.SIZEOF_INT,
                regionNameLength);
              int familyNameLength = buffer.getInt(offset + Bytes.SIZEOF_INT + regionNameLength);
              String familyName = ByteBufferUtils.toStringBinary(buffer,
                offset + 2 * Bytes.SIZEOF_INT + regionNameLength, familyNameLength);
              Pair<String, String> pair = new Pair<String, String>(regionName, familyName);
              short chunkSeqId = buffer.getShort(DurableSlicedChunk.OFFSET_TO_SEQID);
              Map<Short, ByteBuffer> chunkList = this.regionFamilyChunks.get(pair);
              if (chunkList == null) {
                // TODO : this needs refactor. We need to collect cells per family. so we need
                // unique Id per store
                chunkList = new TreeMap<Short, ByteBuffer>();
              }
              this.regionFamilyChunks.put(pair, chunkList);
              chunkList.put(chunkSeqId, buffer);
            } else {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Chunk with id " + chunkId + " not in use");
              }
            }
          }
        }
        extractCellsPerRegion();
        // we need to close it here other wise the new chunk creator cannot
        // create a durable memory area with the same path.
        close();
      }
    }
  }

  private void extractCellsPerRegion() {
    // now we have a sorted set of chunks for each region and its family
    Set<Entry<Pair<String, String>, Map<Short, ByteBuffer>>> entrySet =
        regionFamilyChunks.entrySet();
    for (Entry<Pair<String, String>, Map<Short, ByteBuffer>> entry : entrySet) {
      List<Cell> cellsPerFamily = this.regionFamilyCells.get(entry.getKey());
      if (cellsPerFamily == null) {
        cellsPerFamily = new ArrayList<Cell>();
      }
      this.regionFamilyCells.put(entry.getKey(), cellsPerFamily);
      Collection<ByteBuffer> values = entry.getValue().values();
      List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(values);
      ByteBuffer firstBuffer = buffers.get(0);
      long metaData = firstBuffer.getLong(DurableSlicedChunk.OFFSET_TO_OFFSETMETA);
      int lastChunkSeqId = (int)(metaData >> Integer.SIZE);
      int endOffset = (int)(Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ metaData);
      int offset = DurableSlicedChunk.OFFSET_TO_SEQID + DurableSlicedChunk.SIZE_OF_SEQID
          + DurableSlicedChunk.SIZE_OF_OFFSETMETA;
      // already the buffers should be ordered based on chunk's seqID
      for (ByteBuffer buffer : buffers) {
        // TODO : Again getting these infos. Can avoid
        int regionNameLength = buffer.getInt(offset);
        int familyNameLength =
            buffer.getInt(offset + Bytes.SIZEOF_INT + regionNameLength);
        short chunkSeqId = buffer.getShort(DurableSlicedChunk.OFFSET_TO_SEQID);
        int dataStartOffset = offset + 2 * Bytes.SIZEOF_INT + regionNameLength + familyNameLength;
        int off = dataStartOffset;
        // TODO : handle multiple chunk cases. Need to test
        while (off <= endOffset) {
          if (chunkSeqId <= lastChunkSeqId) {
            if (buffer.capacity() - off < Bytes.SIZEOF_INT) {
              // we should have atleast int space to read the key length
              // break the inner loop and go back to the next chunk
              break;
            }
            int keyLength = ByteBufferUtils.toInt(buffer, off);
            // if the buffer was closed out by us forcefully since we did not have enough space
            // -1 indicates forceful close, 0 indicates the batch was over with it
            if (keyLength == -1 || keyLength == 0) {
              break;
            }
            int valueLength = ByteBufferUtils.toInt(buffer, off + Bytes.SIZEOF_INT);
            long seqId = ByteBufferUtils.toLong(buffer,
              off + keyLength + valueLength + (2 * Bytes.SIZEOF_INT));
            // clone it to avoid the references to the durable memory area.
            ExtendedCell cell = new ByteBufferChunkKeyValue(buffer, off,
                keyLength + valueLength + (2 * Bytes.SIZEOF_INT)).deepClone();
            try {
              cell.setSequenceId(seqId);
            } catch (IOException e) {
              // Will not happen
            }
            cellsPerFamily.add(cell);
            // include per cell SeqID also here
            off += keyLength + valueLength + (2 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_LONG;
          }
        }
      }
    }
  }

  public List<Cell> getCellsPerFamily(String regionName, String famName) {
    return this.regionFamilyCells.get(new Pair<String, String>(regionName, famName));
  }

  public void clear() {
    this.regionFamilyCells.clear();
  }

  public void clearRegionChunk(String regionName, String famName) {
    this.regionFamilyCells.remove(new Pair<String, String>(regionName, famName));
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
