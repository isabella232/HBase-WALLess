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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DurableMemStoreLABImpl extends MemStoreLABImpl {

  private static final int SIZE_OF_SEQ_ID = Bytes.SIZEOF_LONG;

  private AtomicReference<DurableSlicedChunk> firstChunk = new AtomicReference<>();
  private volatile short chunkSeqId = 1;

  private volatile int writeSeqId = 1;
  private final LinkedList<WriteEntry> writeQueue = new LinkedList<>();
  private final Object persistOrderingLockObj = new Object();

  // Used in testing
  public DurableMemStoreLABImpl() {
    this(null, null, new Configuration());
  }

  // Used in testing
  public DurableMemStoreLABImpl(Configuration conf) {
    this(null, null, conf);
  }

  public DurableMemStoreLABImpl(byte[] regionName, byte[] cfName, Configuration conf) {
    super(regionName, cfName, conf);
  }

  @Override
  public List<Cell> copyCellsInto(List<Cell> cells) {
    int totalSize = 0;
    for (Cell cell : cells) {
      totalSize += serializedSizeOf(cell);
      totalSize += SIZE_OF_SEQ_ID;// We need to serialize seqId also for durable MSLABs
    }
    DurableSlicedChunk c = null;
    int allocOffset = 0;
    WriteEntry writeEntry;
    while (true) {
      // Try to get the chunk
      c = (DurableSlicedChunk) getOrMakeChunk();
      // we may get null because the some other thread succeeded in getting the lock
      // and so the current thread has to try again to make its chunk or grab the chunk
      // that the other thread created
      // Try to allocate from this chunk
      if (c != null) {
        synchronized (this.persistOrderingLockObj) {
          allocOffset = c.alloc(totalSize);
          if (allocOffset == -1) {
            // We are moving from this chunk. Just mark EO cells in this
            c.markEndOfCells();
          } else {
            // We succeeded - this is the common case - small alloc
            // from a big buffer
            writeEntry = addWriteEntry(c, allocOffset, totalSize, null);
            break;
          }
        }
        return copyCellsIntoMultiChunks(cells);
      }
    }
    return copyCellsToChunk(cells, c, allocOffset, writeEntry);
  }

  private WriteEntry addWriteEntry(DurableSlicedChunk c, int offset, int len,
      List<DurableSlicedChunk> otherChunks) {
    WriteEntry writeEntry = new WriteEntry(this.writeSeqId++, c, offset, len, otherChunks);
    this.writeQueue.add(writeEntry);
    return writeEntry;
  }

  private int serializedSizeOf(Cell cell) {
    return (cell instanceof ExtendedCell) ? ((ExtendedCell) cell).getSerializedSize(true)
        : KeyValueUtil.length(cell);
  }

  private List<Cell> copyCellsToChunk(List<Cell> cells, DurableSlicedChunk c, int allocOffset,
      WriteEntry writeEntry) {
    List<Cell> toReturn = new ArrayList<>(cells.size());
    int cellSize;
    for (Cell cell : cells) {
      cellSize = serializedSizeOf(cell);
      toReturn.add(copyToChunkCell(cell, c.getData(), allocOffset, cellSize));
      allocOffset += cellSize;
      allocOffset += SIZE_OF_SEQ_ID;// We wrote seqId also.
    }
    persistWrite(writeEntry);
    return toReturn;
  }

  @Override
  protected Cell copyToChunkCell(Cell cell, ByteBuffer buf, int offset, int len) {
    Cell newCell = super.copyToChunkCell(cell, buf, offset, len);
    // Write the seqId of Cell
    ByteBufferUtils.putLong(buf, offset + len, newCell.getSequenceId());
    return newCell;
  }

  private List<Cell> copyCellsIntoMultiChunks(List<Cell> cells) {
    // We asked for more space for N cells. Might be like we have to get N chunks for this. Do
    // this under the lock. We may reach here rarely?
    ObjectIntPair<DurableSlicedChunk> offsets[] = new ObjectIntPair[cells.size()];
    List<DurableSlicedChunk> uniqueChunks = new ArrayList<>();
    WriteEntry writeEntry;
    DurableSlicedChunk lastChunk = null;
    int offset = Integer.MAX_VALUE;
    int len= 0;
    this.lock.lock();
    try {
      for (int i = 0; i < cells.size(); i++) {
        int cellSize = serializedSizeOf(cells.get(i)) + SIZE_OF_SEQ_ID;
        ObjectIntPair<DurableSlicedChunk> chunkAndOffset = allocChunk(cellSize);
        offsets[i] = chunkAndOffset;
        if (lastChunk == null || lastChunk != chunkAndOffset.getFirst()) {
          if (lastChunk != null) {
            lastChunk.markEndOfCells();
          }
          uniqueChunks.add(chunkAndOffset.getFirst());
          offset = Integer.MAX_VALUE;
          len = 0;
        }
        lastChunk = chunkAndOffset.getFirst();
        offset = Integer.min(offset, chunkAndOffset.getSecond());
        len += cellSize;
      }
      lastChunk = uniqueChunks.remove(uniqueChunks.size() - 1);
      synchronized (this.persistOrderingLockObj) {
        writeEntry = addWriteEntry(lastChunk, offset, len, uniqueChunks);
      }
    } finally {
      lock.unlock();
    }
    List<Cell> toReturn = new ArrayList<>(cells.size());
    for (int i = 0; i < cells.size(); i++) {
      Cell cell = cells.get(i);
      toReturn.add(copyToChunkCell(cell, offsets[i].getFirst().getData(), offsets[i].getSecond(),
          serializedSizeOf(cell)));
    }
    persistWrite(writeEntry);
    return toReturn;
  }

  private void persistWrite(WriteEntry writeEntry) {
    writeEntry.complete();
    synchronized (this.persistOrderingLockObj) {
      List<WriteEntry> completedWrites = new ArrayList<>();
      while (!writeQueue.isEmpty()) {
        if (this.writeQueue.getFirst().isCompleted()) {
          completedWrites.add(this.writeQueue.removeFirst());
        } else {
          break;
        }
      }
      if (completedWrites.isEmpty()) {
        // A prior write has not yet finished. We have to wait unless that is done for the
        // persist. Some other write thread will persist our part also later. So just wait for it
        waitForPriorWritesCompletion(writeEntry);
      } else {
        persistBulkOfWrites(completedWrites);
        this.persistOrderingLockObj.notifyAll();
        if (completedWrites.get(completedWrites.size() - 1).seqNo < writeEntry.seqNo) {
          waitForPriorWritesCompletion(writeEntry);
        }
      }
    }
  }

  private void persistBulkOfWrites(List<WriteEntry> completedWrites) {
    DurableSlicedChunk lastChunk = null;
    int offset = Integer.MAX_VALUE;
    int len= 0;
    for (WriteEntry completedWrite : completedWrites) {
      if (completedWrite.prevChunks != null) {
        for (DurableSlicedChunk chunk : completedWrite.prevChunks) {
          chunk.persist();
        }
      }
      if (lastChunk != null && lastChunk != completedWrite.chunk) {
        lastChunk.persist();
        offset = Integer.MAX_VALUE;
        len = 0;
      }
      lastChunk = completedWrite.chunk;
      offset = Integer.min(offset, completedWrite.offset);
      len += completedWrite.len;
    }
    lastChunk.persist(offset, len);
    // Update the meta data in the first chunk
    // TODO Confirm below logic is correct and then removed the commented lines
    long meta = lastChunk.getId();
    meta = (meta << 32) + (int) (offset + len);
    ByteBufferUtils.putLong(this.firstChunk.get().data, DurableSlicedChunk.OFFSET_TO_OFFSETMETA,
        meta);
    /*ByteBufferUtils.putInt(this.firstChunk.get().data, DurableSlicedChunk.OFFSET_TO_OFFSETMETA,
        lastChunk.getId());
    ByteBufferUtils.putInt(this.firstChunk.get().data,
        DurableSlicedChunk.OFFSET_TO_OFFSETMETA + Bytes.SIZEOF_INT, (int) (offset + len));*/
    this.firstChunk.get().persist(DurableSlicedChunk.OFFSET_TO_OFFSETMETA,
        DurableSlicedChunk.SIZE_OF_OFFSETMETA);
  }

  private void waitForPriorWritesCompletion(WriteEntry writeEntry) {
    while (true) {
      synchronized (this.persistOrderingLockObj) {
        try {
          this.persistOrderingLockObj.wait();
        } catch (InterruptedException e) {
          // TODO need to handle any?
        }
        // We are been acked. Check now whether our write is persisted.
        if (!this.writeQueue.isEmpty() && (this.writeQueue.getFirst().seqNo > writeEntry.seqNo)) {
          break;
        }
        // we are not yet done. Just continue to wait!
      }
    }
  }

  private ObjectIntPair<DurableSlicedChunk> allocChunk(int size) {
    DurableSlicedChunk c = null;
    int allocOffset = 0;
    while (true) {
      // Try to get the chunk
      c = (DurableSlicedChunk) getOrMakeChunk();
      // we may get null because the some other thread succeeded in getting the lock
      // and so the current thread has to try again to make its chunk or grab the chunk
      // that the other thread created
      // Try to allocate from this chunk
      if (c != null) {
        allocOffset = c.alloc(size);
        if (allocOffset != -1) {
          // We succeeded - this is the common case - small alloc
          // from a big buffer
          break;
        }
        // not enough space!
        // try to retire this chunk
        tryRetireChunk(c);
      }
    }
    return new ObjectIntPair<>(c, allocOffset);
  }

  @Override
  protected void processNewChunk(Chunk c) {
    // Add seqId into this chunk
    // We call this under lock. So the seqId need not be a thread safe state.
    ByteBufferUtils.putShort(c.data, DurableSlicedChunk.OFFSET_TO_SEQID, chunkSeqId++);
    assert c instanceof DurableSlicedChunk;
    ((DurableSlicedChunk) c).persist(DurableSlicedChunk.OFFSET_TO_SEQID,
        DurableSlicedChunk.SIZE_OF_SEQID);
    this.firstChunk.compareAndSet(null, (DurableSlicedChunk) c);
  }

  private class WriteEntry {
    private int seqNo;
    private DurableSlicedChunk chunk;
    private int offset;
    private int len;
    private List<DurableSlicedChunk> prevChunks;
    private volatile boolean completed = false;

    WriteEntry(int seqNo, DurableSlicedChunk chunk, int offset, int len,
        List<DurableSlicedChunk> prevChunks) {
      this.seqNo = seqNo;
      this.chunk = chunk;
      this.offset = offset;
      this.len = len;
      this.prevChunks = prevChunks;
    }

    void complete() {
      this.completed = true;
    }

    boolean isCompleted() {
      return this.completed;
    }
  }
}
