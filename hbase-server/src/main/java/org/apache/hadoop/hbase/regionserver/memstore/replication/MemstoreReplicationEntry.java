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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public class MemstoreReplicationEntry {
  private final MemstoreReplicationKey memstoreReplicationKey;
  private final MemstoreEdits memstoreEdits;
  private CompletedFuture future;
  private long seq;

  public MemstoreReplicationEntry(MemstoreReplicationKey memstoreRepKey,
      MemstoreEdits memstoreEdits) {
    this.memstoreReplicationKey = memstoreRepKey;
    this.memstoreEdits = memstoreEdits;
  }

  public MemstoreReplicationKey getMemstoreReplicationKey() {
    return this.memstoreReplicationKey;
  }

  public MemstoreEdits getMemstoreEdits() {
    return this.memstoreEdits;
  }

  public void attachFuture(CompletedFuture future, long seq) {
    this.future = future;
    this.seq = seq;
  }

  public CompletedFuture getFuture() {
    return this.future;
  }

  public long getSeq() {
    return this.seq;
  }
}
