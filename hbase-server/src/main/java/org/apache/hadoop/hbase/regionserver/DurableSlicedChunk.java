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

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.mnemonic.ChunkBuffer;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static final int SIZE_OF_CELL_SEQ_ID = Bytes.SIZEOF_LONG;
  public static final int UNUSED_SEQID = 0;
  public static final int OFFSET_TO_SEQID = Bytes.SIZEOF_INT;
  public static final int SIZE_OF_SEQID = Bytes.SIZEOF_INT;
  public static final int OFFSET_TO_OFFSETMETA = OFFSET_TO_SEQID + SIZE_OF_SEQID;
  public static final int SIZE_OF_OFFSETMETA = Bytes.SIZEOF_LONG;
  public static final int OFFSET_TO_REGION_IDENTIFIER = OFFSET_TO_OFFSETMETA + SIZE_OF_OFFSETMETA; 
  private static final int EO_CELLS = 0;

  static final Logger LOG = LoggerFactory.getLogger(DurableSlicedChunk.class);
  private DurableChunk<NonVolatileMemAllocator> durableChunk;
  private ChunkBuffer chunkBuffer;
  private long offset;
  private static byte[] dummy;

  public DurableSlicedChunk(int id, DurableChunk<NonVolatileMemAllocator> durableBigChunk,
      long offset, int size, ChunkCreator chunkCreator) {
    super(size, id, true, chunkCreator);// Durable chunks are always created out of pool.
    this.offset = offset;
    this.durableChunk = durableBigChunk;
    // All DurableSlicedChunk are created at the RS startup time itself in a sequential way. No
    // worry abt concurrent access.
    if (dummy == null) {
      dummy = new byte[this.size];
    }
  }

  @Override
  int allocateDataBuffer(byte[] regionName, byte[] cfName) {
    if (data == null) {
      chunkBuffer = durableChunk.getChunkBuffer(offset, size);
      data = chunkBuffer.get();
      ByteBufferUtils.putInt(data, 0, this.getId());// Write the chunk ID
    }
    // createBuffer.cancelAutoReclaim(); this causes NPE
    // fill the data here
    // Every chunk will have
    // 1) The chunk id (integer)
    // 2) SeqId Int value representing seqId of this chunk within this region:cf
    // 3) The end offset - A long value representing upto which the data was actually synced.
    //    4 bytes of last chunk's seqId followed by 4 bytes of offset within it. 
    // 4) The region name (Region Name length as int and then name bytes)
    // 5) The CF name (CF Name length as int and then name bytes)

    int offset = OFFSET_TO_REGION_IDENTIFIER;
    if (regionName != null) {
      assert cfName != null;
      ByteBufferUtils.putInt(data, offset, regionName.length);// Write regionName
      offset += Bytes.SIZEOF_INT;
      ByteBufferUtils.copyFromArrayToBuffer(this.data, offset, regionName, 0, regionName.length);
      offset += regionName.length;
      ByteBufferUtils.putInt(data, offset, cfName.length);// Write cfName
      offset += Bytes.SIZEOF_INT;
      ByteBufferUtils.copyFromArrayToBuffer(this.data, offset, cfName, 0, cfName.length);
      offset += cfName.length;
      chunkBuffer.syncToLocal(OFFSET_TO_REGION_IDENTIFIER, (offset - OFFSET_TO_REGION_IDENTIFIER));
    }
    return offset;
  }

  void prepopulateChunk() {
    // We have observed that the write perf of the persistent memory is much higher when the chunk
    // area is prepopulated with data. So this hack!
    ByteBufferUtils.copyFromArrayToBuffer(data, Bytes.SIZEOF_INT, dummy, 0,
        (size - Bytes.SIZEOF_INT));
  }

  /**
   * @return The name of the region:cf which this chunk is been assigned to. Null if it is not
   *         having data from any of the region:cf.
   */
  // TODO use BBUtils
  Pair<byte[], byte[]> getOwnerRegionStore() {
    int seqId = ByteBufferUtils.toInt(data, OFFSET_TO_SEQID);
    // TODO On a fresh file based chunk area, chances of we getting a >0 integer is likely. We need
    // a better way to know whether this is a fresh file open or second time. A file level meta data
    // will work? 
    if (seqId > UNUSED_SEQID) {
      int offset = OFFSET_TO_REGION_IDENTIFIER;
      int regionNameLen = ByteBufferUtils.toInt(data, offset);
      byte[] regionName =  new byte[regionNameLen];
      offset += Bytes.SIZEOF_INT;
      ByteBufferUtils.copyFromBufferToArray(regionName, this.data, offset, 0, regionNameLen);
      offset += regionNameLen;
      int cfNameLen = ByteBufferUtils.toInt(data, offset);
      byte[] cfName =  new byte[cfNameLen];
      offset += Bytes.SIZEOF_INT;
      ByteBufferUtils.copyFromBufferToArray(cfName, this.data, offset, 0, cfNameLen);
      return new Pair<byte[], byte[]>(regionName, cfName);
    }
    return null;
  }

  @Override
  public void prePutbackToPool() {
    // TODO check why write using BBUtils not working in some cases.
    ByteBufferUtils.putInt(data, OFFSET_TO_SEQID, UNUSED_SEQID);
    chunkBuffer.syncToLocal(OFFSET_TO_SEQID, SIZE_OF_SEQID);
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
  void markEndOfCells() {
    // We move from one chunk to another. Mark in the prev chunk at the end of last cell.
    // Write a Key length as -1 to denote this is the end of cells here. In replay we
    // consider this.
    if (isEOCToBeMarked(this.nextFreeOffset.get())) {
      ByteBufferUtils.putInt(this.data, this.nextFreeOffset.get(), EO_CELLS);
    }
  }

  int getSeqId() {
    // TODO work with BBUtils?
    return ByteBufferUtils.toInt(data, OFFSET_TO_SEQID);
  }

  Pair<Integer, Integer> getCellsOffsetMeta() {
    if (this.getSeqId() == 0) {
      throw new IllegalStateException();
    }
    // TODO work with BBUtils?
    long meta = ByteBufferUtils.toLong(data, OFFSET_TO_OFFSETMETA);
    int endOffset = (int) (Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ meta);
    int lastChunkSeqId = (int) (meta >> Integer.SIZE);
    return new Pair<Integer, Integer>(lastChunkSeqId, endOffset);
  }

  void writeEndOfCellsOffset(int lastChunkSeqId, int lastOffset) {
    // TODO Confirm below logic is correct and then removed the commented lines
    long meta = lastChunkSeqId;
    meta = (meta << Integer.SIZE) + lastOffset;
    // TODO check why write using BBUtils not working in some cases. May be some Endian issues?
    ByteBufferUtils.putLong(data, OFFSET_TO_OFFSETMETA, meta);
    this.persist(OFFSET_TO_OFFSETMETA, SIZE_OF_OFFSETMETA);
  }

  CellScanner getCellScanner(Optional<Integer> endOffset) {
    CellScanner scanner = new CellScanner() {
      private Cell curCell = null;
      private int offset = getCellOffset();

      @Override
      public Cell current() {
        return this.curCell;
      }

      // TODO use BBUtils APIs
      @Override
      public boolean advance() throws IOException {
        int keyLen, valLen, tagsLen;
        if (endOffset.isPresent() && this.offset >= endOffset.get()) return false;
        int offsetTmp = this.offset;
        if(!isEOCToBeMarked(offsetTmp)) return false;
        keyLen = ByteBufferUtils.toInt(data, offsetTmp);
        if (keyLen == EO_CELLS) return false;
        offsetTmp += KeyValue.KEY_LENGTH_SIZE;
        valLen = ByteBufferUtils.toInt(data, offsetTmp);
        offsetTmp += keyLen + valLen + Bytes.SIZEOF_INT;
        // Read tags
        // TODO : Revisit this
        /*tagsLen = ByteBufferUtils.readAsInt(data, offsetTmp, Tag.TAG_LENGTH_SIZE);
        offsetTmp += (Tag.TAG_LENGTH_SIZE + tagsLen);*/
        long seqId = ByteBufferUtils.toLong(data, offsetTmp);
        this.curCell = new ByteBufferKeyValue(data, this.offset, offsetTmp - this.offset, seqId);
        this.offset = offsetTmp + SIZE_OF_CELL_SEQ_ID;
        return true;
      }
    };
    return scanner;
  }

  private boolean isEOCToBeMarked(int offset) {
    return (this.data.capacity() - offset > KeyValue.KEY_LENGTH_SIZE);
  }

  private int getCellOffset() {
    int offset = OFFSET_TO_REGION_IDENTIFIER;
    int regionNameLen = ByteBufferUtils.toInt(data, offset);
    offset += regionNameLen + Bytes.SIZEOF_INT;
    int cfNameLen = ByteBufferUtils.toInt(data, offset);
    return offset + cfNameLen + Bytes.SIZEOF_INT;
  }
}
