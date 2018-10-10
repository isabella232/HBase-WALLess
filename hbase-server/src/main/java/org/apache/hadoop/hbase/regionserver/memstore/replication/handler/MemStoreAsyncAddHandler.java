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
package org.apache.hadoop.hbase.regionserver.memstore.replication.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.AbstractMemStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemStoreSizing;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl.WriteEntry;
import org.apache.hadoop.hbase.regionserver.memstore.replication.RegionReplicaCordinator;
import org.apache.hadoop.hbase.regionserver.memstore.replication.RegionReplicaStoreCordinator;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MemStoreAsyncAddHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(MemStoreAsyncAddHandler.class);

  private final MemStoreSizing memstoreSizing = new MemStoreSizing();
  private final List<Pair<AbstractMemStore, List<Cell>>> allCell = new ArrayList<>();
  private final WriteEntry[] writeEntries;
  private final HRegion hRegion;
  private final RegionReplicaCordinator replicaCordinator;
  private final long maxSeqId;

  public MemStoreAsyncAddHandler(HRegion hRegion, RegionReplicaCordinator replicaCordinator,
      WriteEntry[] writeEntries, long maxSeqId) {
    super(null, EventType.RS_REGION_REPLICA_MEMSTORE_ASYNC_ADD);
    this.hRegion = hRegion;
    this.replicaCordinator = replicaCordinator;
    this.writeEntries = writeEntries;
    this.maxSeqId = maxSeqId;
  }

  @Override
  public void process() throws IOException {
    for (Pair<AbstractMemStore, List<Cell>> pair : allCell) {
      RegionReplicaStoreCordinator storeCordinator = this.replicaCordinator
          .getStoreCordinator(pair.getFirst().getFamilyName());
      if (storeCordinator.shouldAddCells(this.maxSeqId)) {
        for (Cell cell : pair.getSecond()) {
          // at any point of time if we can check that we have cells that are smaller than the
          // flushed seqId. We return.
          pair.getFirst().internalAdd(cell, memstoreSizing);
        }
        this.hRegion.incMemStoreSize(memstoreSizing);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Dropping cells from adding to Memstore as these cells are already flushed"
              + " in primary region. Max seqId of cells : " + this.maxSeqId);
        }
      }
    }
    for (WriteEntry writeEntry : writeEntries) {
      this.hRegion.getMVCC().complete(writeEntry);
    }
  }

  public void append(AbstractMemStore memstore, List<Cell> cells) {
    this.allCell.add(new Pair<AbstractMemStore, List<Cell>>(memstore, cells));
  }
}
