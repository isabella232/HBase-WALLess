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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.mnemonic.ChunkBuffer;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;

import com.google.common.base.Preconditions;

/**
 * An implementation of Durable chunk
 */
/**
 * Notes on what this chunk should have
 * 1) this chunk should have the region name to which the current chunk is associated. Every MSLabImpl
 * gets a chunk and that chunk is used till it is full. Every MSLAbImpl is created per region and on flush
 * a new one is created.
 * 2) For easy deserialization of a cell we need cell length before every cell.
 * 3) We need to know where the chunk has ended. Either write the end byte after every cell and keep
 * overwriting as and when the next cell comes or write it as the end (as fixed set of bytes).
 * I think going with end bytes is better
 * 4) Another thing to be done is that  while returning this chunk to the pool after a flush we have to mark that this chunk
 * is no longer in use. So that if a server crashes just after this we should not try reading the cells from this chunk
 * as already it has been flushed.
 * 5) We may have to write the seqId also in the Chunks. Otherwise when we need to get back the cell we may not know
 * what is the actual seqId of the cell - TODO - discuss
 */
@InterfaceAudience.Private
public class DurableSlicedChunk extends Chunk {

  private DurableChunk<NonVolatileMemAllocator> durableChunk;
  private long offset;
  private ChunkBuffer chunkBuffer;

  public DurableSlicedChunk(int id, DurableChunk<NonVolatileMemAllocator> durableBigChunk,
      long offset, int size) {
    super(size, id, true);// Durable chunks are always created out of pool.
    this.offset = offset;
    this.durableChunk = durableBigChunk;
  }

  @Override
 public void init(String regionName) {
   assert nextFreeOffset.get() == UNINITIALIZED;
   try {
     allocateDataBuffer(regionName);
   } catch (OutOfMemoryError e) {
     boolean failInit = nextFreeOffset.compareAndSet(UNINITIALIZED, OOM);
     assert failInit; // should be true.
     throw e;
   }
   // Mark that it's ready for use
   // Move 4 bytes since the first 4 bytes are having the chunkid in it
   // indicating that this chunk has been in  use
   boolean initted = false;
    if (regionName != null) {
      initted = nextFreeOffset.compareAndSet(UNINITIALIZED,
        3 * Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE + Bytes.toBytes(regionName).length);
    } else {
      initted = nextFreeOffset.compareAndSet(UNINITIALIZED, Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE);
    }
   // We should always succeed the above CAS since only one thread
   // calls init()!
   Preconditions.checkState(initted, "Multiple threads tried to init same chunk");
 }

  /**
   * Reset the offset to UNINITIALIZED before before reusing an old chunk
   */
  void reset() {
    if (nextFreeOffset.get() != UNINITIALIZED) {
      nextFreeOffset.set(UNINITIALIZED);
      // Indicates this chunk has been in use
      // reset this so that if really we read a chunk back we know if it was an used on or unused one
      if (data != null) {
        data.put(Bytes.SIZEOF_INT, (byte) 0);
      }
      allocCount.set(0);
    }
  }

  @Override
  public int alloc(int size) {
    while (true) {
      int oldOffset = nextFreeOffset.get();
      if (oldOffset == UNINITIALIZED) {
        // The chunk doesn't have its data allocated yet.
        // Since we found this in curChunk, we know that whoever
        // CAS-ed it there is allocating it right now. So spin-loop
        // shouldn't spin long!
        Thread.yield();
        continue;
      }
      if (oldOffset == OOM) {
        // doh we ran out of ram. return -1 to chuck this away.
        return -1;
      }

      //this is the end preamble
      // See whether we have enough space to write the INT representing the length of cell,
      // the actual cell and the seqId following it
      if (oldOffset + size + Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG > data.capacity()) {
        return -1; // alloc doesn't fit
      }
      // Try to atomically claim this chunk
      if (nextFreeOffset.compareAndSet(oldOffset,
        oldOffset + size + Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG)) {
        // we got the alloc
        allocCount.incrementAndGet();
        return oldOffset;
      }
      // we raced and lost alloc, try again
    }
  }
  @Override
  void allocateDataBuffer(String regionName) {
    if (data == null) {
      // fill the data here
      // this causes NPE
      // createBuffer.cancelAutoReclaim();
      // Every chunk will have
      // 1) the chunk id (integer)
      // 2) a byte representing if the chunk is in use or not (0 or 1)
      // 3) the end offset - upto which the data was actually written (integer)
      // 4) an integer representing the region name (integer)
      // 5) the actual region name
      chunkBuffer = durableChunk.getChunkBuffer(offset, size);
      data = chunkBuffer.get();
      data.putInt(0, this.getId());
      // Indicates this chunk has been in use
      data.put(Bytes.SIZEOF_INT, (byte)1);
      // next 4 bytes will inidcate the endPreamble.  will be filled in after every cell is written
      // this should be int or short?
      if (regionName != null) {
        LOG.info("Creating new buffer again for region " + regionName);
      }
      if (regionName != null) {
        data.putInt(2* Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE, regionName.length());
        // better pass the name in byte[] only instead of string
        byte[] bytes = Bytes.toBytes(regionName);
        ByteBufferUtils.copyFromArrayToBuffer(data,
          3 * Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE, bytes, 0, bytes.length);
      }
    }
  }

  void persist() {
    this.chunkBuffer.syncToNonVolatileMemory();
    this.chunkBuffer.syncToLocal();
  }
}
