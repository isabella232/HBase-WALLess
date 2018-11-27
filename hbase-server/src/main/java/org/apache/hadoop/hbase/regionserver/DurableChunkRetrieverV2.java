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
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.ChunkCreator.MemStoreChunkPool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class DurableChunkRetrieverV2 {

  private static final Logger LOG = LoggerFactory.getLogger(DurableChunkRetrieverV2.class);

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

  // TODO need some chore service which will check against HM whether now these kept chunks can be
  // released. It can so happen that the replica regions are there many RS which all restarted. The
  // primary replica will get opened in one of the RS. Then in other RS also the chunks can be
  // released. Chore will check to HM whether the primary is up and Good. If so release chunks.
  private NavigableMap<byte[], NavigableMap<byte[], List<DurableSlicedChunk>>> chunks =
      new TreeMap<>(Bytes.BYTES_COMPARATOR);
  private NavigableSet<byte[]> regionsToIgnore = new TreeSet<>(Bytes.BYTES_COMPARATOR);
  private HRegionServer hrs;

  public DurableChunkRetrieverV2(HRegionServer hrs) {
    this.hrs = hrs;
  }
 
  public boolean appendChunk(Pair<byte[], byte[]> regionStore, DurableSlicedChunk chunk) {
    byte[] primaryRegionName = null;
    try {
      primaryRegionName = RegionInfo.toPrimaryRegionName(regionStore.getFirst());
    } catch (IOException e) {
      // Do not expect this to happen.
    }
    NavigableMap<byte[], List<DurableSlicedChunk>> storeVsChunks = this.chunks
        .get(primaryRegionName);
    if (storeVsChunks != null) {
      // Already we know this region is not online in other servers. Means we need to keep it for
      // later replay!
      addChunkUnderStore(regionStore.getSecond(), chunk, storeVsChunks);
    } else {
      if (this.regionsToIgnore.contains(primaryRegionName)) {
        // We already checked abt this region and see it is already online in another RS. So just
        // ignore this chunk. It can be used by any one now.
        return false;
      } else {
        // TODO: We are doing for every region. Should we read through all the chunks and collect the
        // region names and then issue one shot to the master?
        boolean toBeKept = (this.hrs != null) ? !(this.hrs.atleastOneReplicaGood(primaryRegionName))
            : true;
        if (toBeKept) {
          storeVsChunks = new TreeMap<>(Bytes.BYTES_COMPARATOR);
          this.chunks.put(primaryRegionName, storeVsChunks);
          addChunkUnderStore(regionStore.getSecond(), chunk, storeVsChunks);
        } else {
          this.regionsToIgnore.add(primaryRegionName);
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

  public void finishRegionReplay(byte[] region, MemStoreChunkPool dataPool) {
    if (chunks != null) {
      NavigableMap<byte[], List<DurableSlicedChunk>> storeChunks = this.chunks.remove(region);
      if (storeChunks != null) {
        for (List<DurableSlicedChunk> chunks : storeChunks.values()) {
          for (DurableSlicedChunk chunk : chunks) {
            dataPool.putbackChunks(chunk);
          }
        }
      }
    }
  }
}
