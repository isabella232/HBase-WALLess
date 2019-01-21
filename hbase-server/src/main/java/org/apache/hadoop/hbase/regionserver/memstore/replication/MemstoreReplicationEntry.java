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
package org.apache.hadoop.hbase.regionserver.memstore.replication;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos.ReplicateMemstoreResponse;

@InterfaceAudience.Private
public class MemstoreReplicationEntry {
  private static final Logger LOG = LoggerFactory.getLogger(MemstoreReplicationEntry.class);
  private final MemstoreReplicationKey memstoreReplicationKey;
  private final MemstoreEdits memstoreEdits;
  private CompletableFuture<ReplicateMemstoreResponse> future;
  private long seq;
  private boolean metaMarkerReq;
  private int currentSize;

  public MemstoreReplicationEntry(MemstoreReplicationKey memstoreRepKey,
      MemstoreEdits memstoreEdits, boolean metaMarkerReq) {
    this(memstoreRepKey, memstoreEdits, 0, metaMarkerReq);
  }
  
  public MemstoreReplicationEntry(MemstoreReplicationKey memstoreRepKey,
      MemstoreEdits memstoreEdits, int size, boolean metaMarkerReq) {
    this.memstoreReplicationKey = memstoreRepKey;
    this.memstoreEdits = memstoreEdits;
    this.metaMarkerReq = metaMarkerReq;
    this.currentSize = size;
  }

  public MemstoreReplicationKey getMemstoreReplicationKey() {
    return this.memstoreReplicationKey;
  }

  public MemstoreEdits getMemstoreEdits() {
    return this.memstoreEdits;
  }

  public void attachFuture(CompletableFuture<ReplicateMemstoreResponse> future, long seq) {
    this.future = future;
    this.seq = seq;
  }

  public void markResponse(ReplicateMemstoreResponse response) {
    boolean ret = this.future.complete(response);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Marking the response for the seqID " + seq + " "
          + Bytes.toString(this.memstoreReplicationKey.getEncodedRegionName()) + " " + ret);
    }
  }

  public void markException(IOException e) {
    this.future.completeExceptionally(e);
  }

  public long getSeq() {
    return this.seq;
  }

  public boolean isMetaMarkerReq() {
    return this.metaMarkerReq;
  }

  public int getCurrentMemstoreEntrySize() {
    return this.currentSize;
  }
}
