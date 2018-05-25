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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.mnemonic.ChunkBuffer;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Notes on what this chunk should have
 * 1) this chunk should have the region name to which the current chunk is associated.
 * Every MSLabImpl gets a chunk and that chunk is used till it is full. Every MSLAbImpl is created
 * per region and on flush a new one is created.
 * 2) For easy deserialization of a cell we need cell length before every cell.
 * 3) We need to know where the chunk has ended. Either write the end byte after every cell and keep
 * overwriting as and when the next cell comes or write it as the end (as fixed set of bytes).
 * I think going with end bytes is better
 * 4) Another thing to be done is that  while returning this chunk to the pool after a
 * flush we have to mark that this chunk is no longer in use. So that if a server crashes
 * just after this we should not try reading the cells from this chunk as already
 * it has been flushed.
 * 5) We may have to write the seqId also in the Chunks. Otherwise when we need to get back
 * the cell we may not know
 * what is the actual seqId of the cell - TODO - discuss
 */
@InterfaceAudience.Private
public class DurableSlicedChunk extends Chunk {

  public static final int OFFSET_TO_SEQID = Bytes.SIZEOF_INT;
  public static final int SIZE_OF_SEQID = Bytes.SIZEOF_SHORT;
  public static final int OFFSET_TO_OFFSETMETA = OFFSET_TO_SEQID + SIZE_OF_SEQID;
  public static final int SIZE_OF_OFFSETMETA = Bytes.SIZEOF_LONG;
  private static final int EO_CELLS = -1;

  private DurableChunk<NonVolatileMemAllocator> durableChunk;
  private ChunkBuffer chunkBuffer;
  private long offset;

  public DurableSlicedChunk(int id, DurableChunk<NonVolatileMemAllocator> durableBigChunk,
      long offset, int size) {
    super(size, id, true);// Durable chunks are always created out of pool.
    this.offset = offset;
    this.durableChunk = durableBigChunk;
  }

  @Override
  int allocateDataBuffer(byte[] regionName, byte[] cfName) {
    if (data == null) {
      chunkBuffer = durableChunk.getChunkBuffer(offset, size);
      data = chunkBuffer.get();
      data.putInt(0, this.getId());// Write the chunk ID
    }
    // createBuffer.cancelAutoReclaim(); this causes NPE
    // fill the data here
    // Every chunk will have
    // 1) The chunk id (integer)
    // 2) SeqId Short value representing seqId of this chunk within this region:cf
    // 3) The end offset - A long value representing upto which the data was actually synced.
    //    4 bytes of last chunk's seqId followed by 4 bytes of offset within it. 
    // 4) The region name (Region Name length as int and then name bytes)
    // 5) The CF name (CF Name length as int and then name bytes)

    // 4 bytes taken by chunkId and 2 bytes by seqId for this chunk. ChunkId at the global
    // ChunkCreator level where as the seqId is at MSLAB impl level sequencing.
    int offset = OFFSET_TO_SEQID + SIZE_OF_SEQID + SIZE_OF_OFFSETMETA;
    // next 4 bytes will inidcate the endPreamble. will be filled in after every cell is written
    // this should be int or short?
    if (regionName != null) {
      assert cfName != null;
      data.putInt(offset, regionName.length);// Write regionName
      offset += Bytes.SIZEOF_INT;
      ByteBufferUtils.copyFromArrayToBuffer(this.data, offset, regionName, 0, regionName.length);
      offset += regionName.length;
      data.putInt(offset, cfName.length);// Write cfName
      offset += Bytes.SIZEOF_INT;
      ByteBufferUtils.copyFromArrayToBuffer(this.data, offset, cfName, 0, cfName.length);
      offset += cfName.length;
      chunkBuffer.syncToLocal(0, offset);
      // Next 4 bytes will be used for storing the end offset until which the cells are added.
    }
    return offset;
  }

  @Override
  public void prePutbackToPool() {
    // mark the 4 bytes where we mark the end offset as -1. So that we can use this value to decide
    // if the chunk was flushed
    //  TODO check once.
    data.putInt(OFFSET_TO_SEQID + SIZE_OF_SEQID + Bytes.SIZEOF_INT, 0);
    // sync this value
    chunkBuffer.syncToLocal(OFFSET_TO_SEQID + SIZE_OF_SEQID + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
  }

  public void persist(long offset, int len) {
    this.chunkBuffer.syncToLocal(offset, len);
  }

  public void persist() {
    this.chunkBuffer.syncToLocal();
  }

  /*
   * Not thread safe. Should be called accordingly.
   */
  public void markEndOfCells() {
    // We move from one chunk to another. Mark in the prev chunk at the end of last cell.
    // Write a Key length as -1 to denote this is the end of cells here. In replay we
    // consider this.
    if (this.data.capacity() - this.nextFreeOffset.get() >= KeyValue.KEY_LENGTH_SIZE) {
      ByteBufferUtils.putInt(this.data, this.nextFreeOffset.get(), EO_CELLS);
    }
  }
}
