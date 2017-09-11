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
package org.apache.hadoop.hbase.regionserver.memstore.replication;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateMemstoreReplicaEntryResponse;

@InterfaceAudience.Private
public class CompletedFuture {
  volatile boolean completed = false;
  // TODO : currently not marking for exception. We may need this inside the response only
  volatile Throwable throwable = null;
  volatile ReplicateMemstoreReplicaEntryResponse response = null;

  public synchronized boolean isDone() {
    return this.completed;
  }

  public synchronized ReplicateMemstoreReplicaEntryResponse get(long timeoutNs)
      throws InterruptedException, TimeoutIOException, ExecutionException {
    final long done = System.nanoTime() + timeoutNs;
    while (!isDone()) {
      wait(1000);
      if (System.nanoTime() >= done) {
        throw new TimeoutIOException(
            "Failed to get sync result after " + TimeUnit.NANOSECONDS.toMillis(timeoutNs) + " ms ");
      }
    }
    if (this.throwable != null) {
      throw new ExecutionException(this.throwable);
    }
    return this.response;
  }

  public synchronized void markDone() {
    this.completed = true;
    notify();
  }

  public synchronized void markException(Throwable t) {
    this.throwable = t;
    this.completed = true;
    notify();
  }

  public synchronized void markResponse(ReplicateMemstoreReplicaEntryResponse response) {
    this.response = response;
    this.completed = true;
    notify();
  }

  public synchronized CompletedFuture reset() {
    this.completed = false;
    this.response = null;
    notify();
    return this;
  }

  // Use this in case of exception
  public synchronized boolean hasException() {
    return this.throwable != null;
  }
}