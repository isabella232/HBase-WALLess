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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hbase.Cell;

/**
 * A simple POJO like class that holds the different entities required to copy a batch of mutations
 * to MSLAB area
 */
public class MemstoreBatcher {

  private final HRegion region;
  private final MemstoreSize memstoreSize;
  private final Set<Entry<byte[], List<Cell>>> familyVsCells;
  private final Map<byte[], Integer> batchSizePerFamily;
  private final long seqId;
  private final CompletableFuture<Long> completableFuture;
  private boolean markComplete = false;
  private boolean async = false;
  private Action action  = null;

  public MemstoreBatcher(HRegion region, Set<Entry<byte[], List<Cell>>> entrySet,
      Map<byte[], Integer> batchSizePerFamily, MemstoreSize memstoreSize, long seqId) {
    this.region = region;
    this.familyVsCells = entrySet;
    this.batchSizePerFamily = batchSizePerFamily;
    this.memstoreSize = memstoreSize;
    this.seqId = seqId;
    this.completableFuture = new CompletableFuture<Long>();
  }

  public MemstoreBatcher(HRegion region, Set<Entry<byte[], List<Cell>>> entrySet,
      Map<byte[], Integer> batchSizePerFamily, MemstoreSize memstoreSize, long seqId,
      boolean async) {
    this(region, entrySet, batchSizePerFamily, memstoreSize, seqId);
    this.async = async;
  }

  public HRegion getRegion() {
    return this.region;
  }

  public Set<Entry<byte[], List<Cell>>> getCells() {
    return this.familyVsCells;
  }

  public Map<byte[], Integer> getBatchSizePerFamily() {
    return this.batchSizePerFamily;
  }

  public MemstoreSize getMemstoresize() {
    return this.memstoreSize;
  }

  public long getSeqId() {
    return this.seqId;
  }

  public CompletableFuture<Long> getFuture() {
    return this.completableFuture;
  }

  public synchronized void markComplete() {
    this.markComplete = true;
    notifyAll();
  }

  public synchronized void waitForComplete() {
    while (!this.markComplete) {
      try {
        wait(100);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public boolean isAsync() {
    return this.async;
  }

  public void setAction(Action action) {
    this.action = action;
  }

  public Action getAction() {
    return this.action;
  }
}
