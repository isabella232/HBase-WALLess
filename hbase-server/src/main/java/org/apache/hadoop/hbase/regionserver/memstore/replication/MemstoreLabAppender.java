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
// TODO : Move this as to HRegion package?? So that updatesLock need not be exposed
package org.apache.hadoop.hbase.regionserver.memstore.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.HasThread;

/**
 * Responsible for adding and persisting the mutations to the memstore  LABs. It has threads
 * for doing the actual data copy to the MSLAB and threads to do the persistence (syncing)
 * the MSLABs to the pmem devices
 */
public class MemstoreLabAppender {

 /* private final Configuration conf;
  private final RegionServerServices rs;
  private final Appender[] appenderThreads;
  private final Persistor[] persistorThreads;
  private AtomicInteger appendorThreadIndex = new AtomicInteger(0);
  private AtomicInteger persistorThreadIndex = new AtomicInteger(0);

  public MemstoreLabAppender(Configuration conf, RegionServerServices rs) {
    this.conf = conf;
    this.rs = rs;
    int numAppenderThreads = this.conf.getInt("hbase.regionserver.memstore.appender.threads", 16);
    this.appenderThreads = new Appender[numAppenderThreads];
    for (int i = 0; i < numAppenderThreads; i++) {
      this.appenderThreads[i] = new Appender();
      this.appenderThreads[i].start();
    }

    int numPersistorThreads = this.conf.getInt("hbase.regionserver.memstore.persistor.threads", 16);
    this.persistorThreads = new Persistor[numPersistorThreads];
    for (int i = 0; i < numPersistorThreads; i++) {
      this.persistorThreads[i] = new Persistor();
      this.persistorThreads[i].start();
    }
  }

  *//**
   * Helps in copying the cells to the MSLAB chunks. Per region the mutations are
   * grouped and per region the mutations are applied serially.
   * We name it as appender because per  MSLAB the data is added serially in increasing
   * order as a batch.
   *//*
  private static class Appender extends HasThread {

    private volatile boolean closed = false;
    private final BlockingQueue<MemstoreBatcher> regionQueue;

    public Appender() {
      regionQueue = new LinkedBlockingQueue<MemstoreBatcher>();
    }

    @Override
    public void run() {
      try {
        // todo : add exception on interruptoin and shutdown
        while (true) {
          MemstoreBatcher batch = regionQueue.take();
          if (batch != null) {
            Set<Entry<byte[], List<Cell>>> cells = batch.getCells();
            List<Action> actions = new ArrayList<>(cells.size());
            for (Entry<byte[], List<Cell>> entry : cells) {
              // System.out.println("the cells in the appender queue are " + entry.getValue() + " "
              // + Thread.currentThread().getName());
              if (!batch.isAsync()) {
                batch.getRegion().applyToMemstore(batch.getRegion().getHStore(entry.getKey()),
                  entry.getValue(), false, batch.getMemstoresize(),
                  batch.getBatchSizePerFamily().get(entry.getKey()));
              } else {
                Action action = batch.getRegion().applyFamilyMapToMemstoreForMemstoreReplication(
                  batch.getRegion().getHStore(entry.getKey()), entry.getValue(),
                  batch.getMemstoresize(), batch.getBatchSizePerFamily().get(entry.getKey()));
                actions.add(action);
              }
            }
            if (!actions.isEmpty()) {
              batch.setAction(new HRegion.ActionList(actions));
            }
            if (batch.isAsync()) {
              // complete it here only - there is no persist in case of async
              batch.getFuture().complete(batch.getSeqId());
            }
            batch.markComplete();
          } else {
            System.out.println("Got null in appender " + Thread.currentThread().getName());
          }
        }
      } catch (InterruptedException e1) {
        // TODO : handle
        e1.printStackTrace();
      } catch (IOException e) {
        // Won't happen
        e.printStackTrace();
      }
    }
  }

  *//**
   * The MSLAB chunks are persisted to the pmem devices and these threads are per region. 
   * The idea is to reduce the number of sync calls that happen and since different regions share
   * a particular persistor thread, by the time a region persist() happens it could have completed
   * multiple mutation copy to the MSLAB chunk area.
   *//*
  private static class Persistor extends HasThread {

    private volatile boolean closed = false;
    private final BlockingQueue<MemstoreBatcher> regionQueue;

    public Persistor() {
      regionQueue = new LinkedBlockingQueue<MemstoreBatcher>();
    }

    @Override
    public void run() {
      MemstoreBatcher batch;
      // todo : add exception on interruptoin and shutdown
      try {
        while (true) {
          batch = regionQueue.take();
          if (batch != null) {
            Set<Entry<byte[], List<Cell>>> cells = batch.getCells();
            for (Entry<byte[], List<Cell>> entry : cells) {
              batch.getRegion().getHStore(entry.getKey()).persist(batch.getSeqId());
            }
            batch.getFuture().complete(batch.getSeqId());
          } else {
            System.out.println("Got null in persistor " + Thread.currentThread().getName());
          }
        }
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public synchronized int getNextAppender() {
    int res = (appendorThreadIndex.get()) % this.appenderThreads.length;
    appendorThreadIndex.incrementAndGet();
    return res;
  }

  public synchronized int getNextPersistor() {
    int res = (persistorThreadIndex.get()) % this.persistorThreads.length;
    persistorThreadIndex.incrementAndGet();
    return res;
  }

  public void offerForAppend(int appenderIndex, MemstoreBatcher batcher) {
    this.appenderThreads[appenderIndex].regionQueue.offer(batcher);
    return;
  }

  public void offerForPersist(int persistorIndex, MemstoreBatcher batcher) {
    this.persistorThreads[persistorIndex].regionQueue.offer(batcher);
    return;
  }
*/
}
