package org.apache.hadoop.hbase.regionserver.memstore.replication;
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
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MemStoreAsyncAddService {

  public static final String MEMSTORE_REPLICATION_ASYNC_ADD_THREADS =
      "hbase.regionserver.memstore.replication.async.add.threads";
  public static final String MEMSTORE_REPLICATION_ASYNC_ADD_PRIORITY_THREADS =
      "hbase.regionserver.memstore.replication.async.add.priority.threads";

  private static final Logger LOG = LoggerFactory.getLogger(MemStoreAsyncAddService.class);

  private LinkedBlockingQueue<HRegion> regionsQ = new LinkedBlockingQueue<>();

  private final MemstoreAsyncAddHandler[] handlers;

  private ThreadPoolExecutor priorityExecutor;

  public MemStoreAsyncAddService(Configuration conf) {
    int numThreads = conf.getInt(MEMSTORE_REPLICATION_ASYNC_ADD_THREADS, 0);
    this.handlers = new MemstoreAsyncAddHandler[numThreads];
    for (int i = 0; i < numThreads; i++) {
      this.handlers[i] = new MemstoreAsyncAddHandler(i);
      this.handlers[i].start();
    }
    int numPriorityThreads = conf.getInt(MEMSTORE_REPLICATION_ASYNC_ADD_PRIORITY_THREADS, 10);
    BlockingQueue<Runnable> q = new LinkedBlockingQueue<>();
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("MemstoreAsyncAddService-MemstoreAsyncAddPriorityHandler-%1$d");
    builder.setDaemon(true);
    this.priorityExecutor = new ThreadPoolExecutor(numPriorityThreads, numPriorityThreads, 5,
        TimeUnit.SECONDS, q, builder.build());
    this.priorityExecutor.allowCoreThreadTimeOut(true);
  }

  /**
   * Register the given region for the service of adding Cells to memstore CSLM in an async way.
   * @param region
   */
  public void register(HRegion region){
    regionsQ.add (region);
  }

  public boolean deregister(HRegion region) {
    return this.regionsQ.remove(region);
  }

  public void prioritize(HRegion region, long readPnt) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Replica region " + region + " got a read request with readPnt : " + readPnt
          + ". Adding cells async way to CSLM");
    }
    this.priorityExecutor.submit(new MemstoreAsyncAddPriorityHandler(region, readPnt));
  }

  public void close() {
    for (MemstoreAsyncAddHandler handler : this.handlers) {
      handler.close();
    }
    this.priorityExecutor.shutdownNow();
  }

  private class MemstoreAsyncAddHandler extends Thread {
    private boolean closed = false;

    MemstoreAsyncAddHandler(int index) {
      super("MemstoreAsyncAddService-MemstoreAsyncAddHandler-" + index);
      this.setDaemon(true);
    }

    @Override
    public void run() {
      while (!closed) {
        HRegion region = null;
        try {
          region = regionsQ.take();
          if (!stillValidReplicaRegion(region)) {
            continue;
          }
          region.asyncAddToMemstore(Optional.empty());
          if (stillValidReplicaRegion(region)) {
            // Register back the region for later processing. Each processing will handle cells from
            // same batch. Means same seqId
            register(region);
          }
        } catch (InterruptedException e) {
          LOG.info(
              "MemstoreAsyncAddHandler met with InterruptedException while processing region : "
                  + region, e);
        } catch (Throwable t) {
          // Don't let this thread be died.
          LOG.info("MemstoreAsyncAddHandler met with exception while processing region : " + region, t);
        }
      }
    }

    private boolean stillValidReplicaRegion(HRegion region) {
      return (!(region.isClosed() || region.isClosing() || region.isDefaultReplica()));
    }

    public void close() {
      this.closed = true;
      this.interrupt();
    }
  }

  private class MemstoreAsyncAddPriorityHandler implements Runnable {
    private HRegion region;
    private long readPnt;

    MemstoreAsyncAddPriorityHandler(HRegion region, long readPnt) {
      this.region = region;
      this.readPnt = readPnt;
    }

    @Override
    public void run() {
      long seqId = -1;
      while (seqId < this.readPnt) {
        try {
          seqId = region.asyncAddToMemstore(Optional.of(this.readPnt));
        } catch (IOException e) {
          LOG.info("MemstoreAsyncAddPriorityHandler met IOE.", e);
          if (!(e instanceof RegionTooBusyException)) {
            break;
          }
        }
      }
    }
  }
}
