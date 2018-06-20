/**
 *
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
package org.apache.hadoop.hbase.regionserver.memstore.replication.protobuf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreEdits;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreReplicationEntry;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreReplicationKey;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos.ReplicateMemstoreRequest;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MemstoreReplicationProtobufUtil {

  public static Pair<ReplicateMemstoreRequest, List<Cell>> buildReplicateMemstoreEntryRequest(
      final MemstoreReplicationEntry[] entries, byte[] encodedRegionName, List<Integer> pipeline) {
    // Accumulate all the Cells seen in here.
    List<Cell> allCells = new ArrayList<>();
    ReplicateMemstoreRequest.Builder reqBuilder = ReplicateMemstoreRequest.newBuilder();
    int replicasOffsered = 0;
    for (MemstoreReplicationEntry entry : entries) {
      MemstoreReplicationKey key = entry.getMemstoreReplicationKey();
      MemstoreEdits edit = entry.getMemstoreEdits();
      List<Cell> cells = edit.getCells();
      // Collect up the cells
      allCells.addAll(cells);
      MemstoreReplicaProtos.MemstoreReplicationEntry.Builder entryBuilder =
          MemstoreReplicaProtos.MemstoreReplicationEntry.newBuilder();
      entryBuilder.setAssociatedCellCount(cells.size());
      entryBuilder.setSequenceId(key.getSequenceId());
      reqBuilder.addEntry(entryBuilder.build());
      replicasOffsered = key.getReplicasOffered();// Its ok to overwrite. Write comments.. Handle.
                                                  // TODO
    }
    reqBuilder.setEncodedRegionName(UnsafeByteOperations.unsafeWrap(encodedRegionName));
    reqBuilder.setReplicasOffered(replicasOffsered);
    for (int replicaId : pipeline) {
      reqBuilder.addReplicas(replicaId);
    }
    return new Pair<>(reqBuilder.build(), allCells);
  }

  public static CellScanner getCellScannerOnCells(final List<Cell> cells) {
    return new CellScanner() {
      private final Iterator<? extends Cell> entries = cells.iterator();
      private Cell currentCell;

      @Override
      public Cell current() {
        return this.currentCell;
      }

      @Override
      public boolean advance() {
        if (!this.entries.hasNext()) {
          this.currentCell = null;
          return false;
        }
        this.currentCell = this.entries.next();
        return true;
      }
    };
  }
}
