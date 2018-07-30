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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.AbstractMemStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemStoreSizing;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl.WriteEntry;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MemStoreAsyncAddHandler extends EventHandler {

  private final MemStoreSizing memstoreSizing = new MemStoreSizing();
  private final List<Pair<AbstractMemStore, List<Cell>>> allCell = new ArrayList<>();
  private final WriteEntry[] writeEntries;
  private final HRegion hRegion;

  public MemStoreAsyncAddHandler(HRegion hRegion, WriteEntry[] seqIds) {
    super(null, EventType.RS_REGION_REPLICA_MEMSTORE_ASYNC_ADD);
    this.hRegion = hRegion;
    this.writeEntries = seqIds;
  }

  @Override
  public void process() throws IOException {
    for (Pair<AbstractMemStore, List<Cell>> pair : allCell) {
      for (Cell cell : pair.getSecond()) {
        pair.getFirst().internalAdd(cell, memstoreSizing);
      }
    }
    for (WriteEntry writeEntry : writeEntries) {
      this.hRegion.getMVCC().complete(writeEntry);
    }
    this.hRegion.incMemStoreSize(memstoreSizing);
  }

  public void append(AbstractMemStore memstore, List<Cell> cells) {
    this.allCell.add(new Pair<AbstractMemStore, List<Cell>>(memstore, cells));
  }
}
