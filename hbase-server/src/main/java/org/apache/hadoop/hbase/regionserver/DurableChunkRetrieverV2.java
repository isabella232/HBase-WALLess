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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.regionserver.DurableChunkCreator.DurableMemStoreChunkPool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class DurableChunkRetrieverV2 {

  private static final Logger LOG = LoggerFactory.getLogger(DurableChunkRetrieverV2.class);
  private static DurableChunkRetrieverV2 INSTANCE = null;

  private static final CellScanner EMPTY_SCANNER = new CellScanner() {
    @Override
    public Cell current() {
      return null;
    }

    @Override
    public boolean advance() throws IOException {
      return false;
    }
  };

  private DurableMemStoreChunkPool pool;
  private NavigableMap<byte[], NavigableMap<byte[], List<DurableSlicedChunk>>> chunks =
      new TreeMap<>(Bytes.BYTES_COMPARATOR);
  private NavigableSet<byte[]> regionsToIgnore = new TreeSet<>(Bytes.BYTES_COMPARATOR);

  public static synchronized DurableChunkRetrieverV2 init (DurableMemStoreChunkPool pool){
    INSTANCE = new DurableChunkRetrieverV2(pool);
    return INSTANCE;
  }

  public static synchronized DurableChunkRetrieverV2 getInstance() {
    if (INSTANCE == null) {
      throw new IllegalStateException();
    }
    return INSTANCE;
  }

  private DurableChunkRetrieverV2(DurableMemStoreChunkPool pool) {
    this.pool = pool;
  }

  public boolean appendChunk(Pair<byte[], byte[]> regionStore, DurableSlicedChunk chunk) {
    NavigableMap<byte[], List<DurableSlicedChunk>> storeVsChunks = this.chunks
        .get(regionStore.getFirst());
    if (storeVsChunks != null) {
      // Already we know this region is not online in other servers. Means we need to keep it for
      // later replay!
      addChunkUnderStore(regionStore.getSecond(), chunk, storeVsChunks);
    } else {
      if (this.regionsToIgnore.contains(regionStore.getFirst())) {
        // We already checked abt this region and see it is already online in another RS. So just
        // ignore this chunk. It can be used by any one now.
        return false;
      } else {
        boolean toBeKept = true; // TODO Make RPC to check whether this region is online any where
        if (toBeKept) {
          storeVsChunks = new TreeMap<>(Bytes.BYTES_COMPARATOR);
          this.chunks.put(regionStore.getFirst(), storeVsChunks);
          addChunkUnderStore(regionStore.getSecond(), chunk, storeVsChunks);
        } else {
          this.regionsToIgnore.add(regionStore.getFirst());
          return false;
        }
      }
    }
    return true;
  }

  private void addChunkUnderStore(byte[] store, DurableSlicedChunk chunk,
      NavigableMap<byte[], List<DurableSlicedChunk>> storeVsChunks) {
    List<DurableSlicedChunk> storeChunks = storeVsChunks.get(store);
    if (storeChunks == null) {
      storeChunks = new ArrayList<>();
      storeVsChunks.put(store, storeChunks);
    }
    storeChunks.add(chunk);
  }

  // TODO use this while Region replay
  public CellScanner getCellScanner(byte[] region, byte[] store) {
    List<DurableSlicedChunk> storeChunks = getSortedChunks(region, store);
    if (storeChunks.isEmpty()) {
      return EMPTY_SCANNER;
    }
    final Iterator<DurableSlicedChunk> chunksItr = storeChunks.iterator();
    final DurableSlicedChunk firstChunk = chunksItr.next();
    Pair<Integer, Integer> cellsOffsetMeta = firstChunk.getCellsOffsetMeta();
    LOG.debug("For region : {} store : {} cells offset meta : {}", Bytes.toStringBinary(region),
        Bytes.toStringBinary(store), cellsOffsetMeta);
    CellScanner scanner = new CellScanner() {
      private CellScanner curChunkCellScanner = firstChunk
          .getCellScanner(getCellsOffsetForChunk(firstChunk));

      @Override
      public Cell current() {
        return this.curChunkCellScanner.current();
      }

      @Override
      public boolean advance() throws IOException {
        while (true) {
          if (this.curChunkCellScanner.advance()) return true;
          // Cur chunk is over. Move to the next one.
          DurableSlicedChunk chunk = (chunksItr.hasNext()) ? chunksItr.next() : null;
          if (chunk.getSeqId() > cellsOffsetMeta.getFirst()) {
            // We have reached till the last chunk. Just ignore remaining chunks.
            chunk = null;
          }
          if (chunk == null) break;
          this.curChunkCellScanner = chunk.getCellScanner(getCellsOffsetForChunk(chunk));
        }
        return false;
      }

      private Optional<Integer> getCellsOffsetForChunk(DurableSlicedChunk chunk) {
        if (chunk.getSeqId() == cellsOffsetMeta.getFirst()) {
          return Optional.of(cellsOffsetMeta.getSecond());
        }
        return null;
      }
    };
    return scanner;
  }

  private List<DurableSlicedChunk> getSortedChunks(byte[] region, byte[] store) {
    NavigableMap<byte[], List<DurableSlicedChunk>> storeVsChunks = this.chunks.get(region);
    List<DurableSlicedChunk> storeChunks = storeVsChunks == null ? new ArrayList<>()
        : storeVsChunks.get(store);
    // Sort the chunks as per the seqId.
    storeChunks.sort(new Comparator<DurableSlicedChunk>() {
      @Override
      public int compare(DurableSlicedChunk c1, DurableSlicedChunk c2) {
        return c1.getSeqId() - c2.getSeqId();
      }
    });
    return storeChunks;
  }

  // TODO use this.
  public void finishRegionReplay(byte[] region) {
    for (List<DurableSlicedChunk> storeChunks : this.chunks.get(region).values()) {
      for (DurableSlicedChunk chunk : storeChunks) {
        this.pool.putbackChunks(chunk);
      }
    }
  }
}
