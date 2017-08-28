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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.IdReadWriteLock;
import org.apache.hadoop.hbase.util.IdReadWriteLock.ReferenceType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;

import com.google.common.annotations.VisibleForTesting;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * This replicator per region server collects all the {@link MemstoreEdits} and
 * {@link MemstoreReplicationKey} and forms a {@link MemstoreReplicationEntry} per region and uses
 * RingBuffer for execution
 */
@InterfaceAudience.Private
public class RingBufferMemstoreReplicator extends BaseMemstoreReplicator {
  private static final Log LOG = LogFactory.getLog(RingBufferMemstoreReplicator.class);

  private final Disruptor<MemstoreRepRingBufferTruck> disruptor;

  private final RingBufferEventHandler ringEventHandler;

  private ConcurrentHashMap<String, RegionEntryBuffer> regionVsEntryBuffer =
      new ConcurrentHashMap<String, RegionEntryBuffer>();
  @VisibleForTesting
  final IdReadWriteLock<String> offsetLock = new IdReadWriteLock(ReferenceType.SOFT);

  private RegionReplicaOutputSink outputSink;

  class ReplicationThread extends HasThread {
    // add some capacity
    private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
    private MemstoreReplicationEntry entry;
    private OutputSink outputSink = null;

    public ReplicationThread(OutputSink sink) {
      this.outputSink = sink;
    }

    public void offer(String regionName) throws InterruptedException {
      // Keep adding
      queue.put(regionName);
    }

    @Override
    public void run() {
      // The replication call will happen here
      while (true) {
        try {
          String regionName = queue.take();
          ReentrantReadWriteLock writeLock = offsetLock.getLock(regionName);
          RegionEntryBuffer regionEntryBuffer;
          try {
            writeLock.writeLock().lock();
            regionEntryBuffer = regionVsEntryBuffer.remove(regionName);
            // write this with the replicaiton thread
          } finally {
            writeLock.writeLock().unlock();
          }
          if (regionEntryBuffer != null) {
            outputSink.append(regionEntryBuffer);
          }
        } catch (InterruptedException | IOException e) {
          // Hande IOException
        }
      }
    }
  }

  class RingBufferEventHandler implements EventHandler<MemstoreRepRingBufferTruck>, LifecycleAware {
    private final ReplicationThread[] replicationThreads;
    // Had 'interesting' issues when this was non-volatile. On occasion, we'd not pass all
    // syncFutures to the next sync'ing thread.
    private volatile int syncFuturesCount = 0;
    /**
     * Set if we get an exception appending or syncing so that all subsequence appends and syncs on
     * this WAL fail until WAL is replaced.
     */
    private Exception exception = null;
    /**
     * Object to block on while waiting on safe point.
     */
    private volatile boolean shutdown = false;

    /**
     * Which replicationThread to use next.
     */
    private int replicationThreadIndex;

    RingBufferEventHandler(final int syncRunnerCount, final int maxHandlersCount) {
      this.replicationThreads = new ReplicationThread[syncRunnerCount];
      for (int i = 0; i < syncRunnerCount; i++) {
        this.replicationThreads[i] = new ReplicationThread(outputSink);
      }
    }

    @Override
    public void onShutdown() {
      for (ReplicationThread thread : this.replicationThreads) {
        thread.interrupt();
      }
    }

    @Override
    public void onStart() {
      // Start them like the writer threads
      for (ReplicationThread thread : this.replicationThreads) {
        thread.start();
      }
    }

    @Override
    public void onEvent(MemstoreRepRingBufferTruck truck, long txid, boolean endoOfBatch)
        throws Exception {
      // TODO Auto-generated method stub
      MemstoreReplicationEntry memstoreReplicationEntry = truck.getMemstoreReplicationEntry();
      MemstoreReplicationKey memstoreReplicationKey =
          memstoreReplicationEntry.getMemstoreReplicationKey();
      ReentrantReadWriteLock lock = offsetLock.getLock(memstoreReplicationKey.encodedRegionName());
      try {
        // under write lock for this region add to the map
        lock.writeLock().lock();
        RegionEntryBuffer entryBuffer =
            regionVsEntryBuffer.get(memstoreReplicationKey.encodedRegionName());
        if (entryBuffer == null) {
          entryBuffer = new RegionEntryBuffer(memstoreReplicationKey.getTableName(),
              memstoreReplicationKey.getEncodedRegionNameInBytes(),
              memstoreReplicationEntry.getReplicaId());
          regionVsEntryBuffer.put(memstoreReplicationKey.encodedRegionName(), entryBuffer);
        }
        entryBuffer.appendEntry(memstoreReplicationEntry);
        // find a better way once we decide ringBuffer way is faster (Then pass the future in the
        // memstoreRepEntry itself?)
        entryBuffer.appendFuture(truck.getFuture());
      } finally {
        lock.writeLock().unlock();
      }
      this.replicationThreadIndex =
          (this.replicationThreadIndex + 1) % this.replicationThreads.length;
      try {
        this.replicationThreads[this.replicationThreadIndex]
            .offer(memstoreReplicationKey.encodedRegionName());
      } catch (Exception e) {
        // Should NEVER get here.
      }
    }

  }

  public RingBufferMemstoreReplicator(Configuration conf, RegionServerServices rsServices) {
    this.conf = HBaseConfiguration.create(conf);
    this.tableDescriptors = ((HRegionServer) rsServices).getTableDescriptors();
    // HRS multiplies client retries by 10 globally for meta operations, but we do not want this.
    // We are resetting it here because we want default number of retries (35) rather than 10 times
    // that which makes very long retries for disabled tables etc.
    int defaultNumRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    // TODO : what should be the retries here? This is sync flow so cannot be too high also?
    if (defaultNumRetries > 10) {
      int mult = conf.getInt("hbase.client.serverside.retries.multiplier", 10);
      defaultNumRetries = defaultNumRetries / mult; // reset if HRS has multiplied this already
    }

    conf.setInt("hbase.client.serverside.retries.multiplier", 1);
    int numRetries = conf.getInt(CLIENT_RETRIES_NUMBER, defaultNumRetries);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, numRetries);

    // TODO : Like in WAL I think we should have writer threads equal to the number of handlers
    this.numWriterThreads = this.conf.getInt("hbase.region.replica.replication.writer.threads", 3);

    // use the regular RPC timeout for replica replication RPC's
    this.operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    try {
      connection = (ClusterConnection) ConnectionFactory.createConnection(this.conf);
      this.pool = getDefaultThreadPool(conf);
      int numOfReplicationThreads = 5;
      outputSink = new RegionReplicaOutputSink(tableDescriptors, connection, pool,
          numOfReplicationThreads, operationTimeout);
      // outputSink.startWriterThreads();
    } catch (IOException ex) {
      LOG.warn("Received exception while creating connection :" + ex);
    }
    this.replicationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(
      conf.getLong("hbase.regionserver.mutations.sync.timeout", DEFAULT_WAL_SYNC_TIMEOUT_MS));
    this.disruptor = new Disruptor<>(MemstoreRepRingBufferTruck::new, 1024 * 16,
        Threads.getNamedThreadFactory("replicatorthread" + ".append"), ProducerType.MULTI,
        new BlockingWaitStrategy());
    // Advance the ring buffer sequence so that it starts from 1 instead of 0,
    this.disruptor.getRingBuffer().next();
    int maxHandlersCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, 200);
    // need to pass this here
    this.ringEventHandler = new RingBufferEventHandler(5, maxHandlersCount);
    this.disruptor.setDefaultExceptionHandler(new RingBufferExceptionHandler());
    this.disruptor.handleEventsWith(new RingBufferEventHandler[] { this.ringEventHandler });
    // Starting up threads in constructor is a no no; Interface should have an init call.
    this.disruptor.start();
  }

  /**
   * Exception handler to pass the disruptor ringbuffer. Same as native implementation only it logs
   * using our logger instead of java native logger.
   */
  static class RingBufferExceptionHandler implements ExceptionHandler<MemstoreRepRingBufferTruck> {

    @Override
    public void handleEventException(Throwable ex, long sequence,
        MemstoreRepRingBufferTruck event) {
      LOG.error("Sequence=" + sequence + ", event=" + event, ex);
      throw new RuntimeException(ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Returns a Thread pool for the RPC's to region replicas. Similar to Connection's thread pool.
   */
  private ExecutorService getDefaultThreadPool(Configuration conf) {
    int maxThreads = conf.getInt("hbase.region.replica.replication.threads.max", 256);
    if (maxThreads == 0) {
      maxThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    long keepAliveTime = conf.getLong("hbase.region.replica.replication.threads.keepalivetime", 60);
    LinkedBlockingQueue<Runnable> workQueue =
        new LinkedBlockingQueue<>(maxThreads * conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
          HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
    ThreadPoolExecutor tpe =
        new ThreadPoolExecutor(maxThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, workQueue,
            Threads.newDaemonThreadFactory(this.getClass().getSimpleName() + "-rpc-shared-"));
    tpe.allowCoreThreadTimeOut(true);
    return tpe;
  }

  @Override
  public void replicate(MemstoreReplicationKey memstoreReplicationKey, MemstoreEdits memstoreEdits,
      boolean replay, int replicaId) throws IOException, InterruptedException, ExecutionException {
    long txid = -1;
    CompletedFuture future = null;
    try {
      future = new CompletedFuture();
      txid = disruptor.getRingBuffer().next();
      MemstoreReplicationEntry entry =
          new MemstoreReplicationEntry(memstoreReplicationKey, memstoreEdits, replay, replicaId);
      disruptor.getRingBuffer().get(txid).load(entry, future);
    } finally {
      disruptor.getRingBuffer().publish(txid);
    }
    // wait for the result here
    future.get(replicationTimeoutNs);

    // TODO :Very important. Probably return back if there is no replica here instead of putting
    // them in a pool etc.
  }

  static class RegionReplicaOutputSink extends OutputSink {
    private final RegionReplicaSinkWriter sinkWriter;
    private final TableDescriptors tableDescriptors;

    public RegionReplicaOutputSink(TableDescriptors tableDescriptors, ClusterConnection connection,
        ExecutorService pool, int numWriters, int operationTimeout) {
      super(null, null, numWriters);
      this.sinkWriter = new RegionReplicaSinkWriter(this, connection, pool, operationTimeout);
      this.tableDescriptors = tableDescriptors;
    }

    @Override
    public void append(RegionEntryBuffer buffer) throws IOException {
      List<MemstoreReplicationEntry> entries = buffer.getEntryBuffer();
      List<CompletedFuture> futures = buffer.getFutures();

      if (entries.isEmpty() || entries.get(0).getMemstoreEdits().getCells().isEmpty()) {
        return;
      }
      boolean replay = entries.get(0).isReplay();
      int replicaId = entries.get(0).getReplicaId();
      sinkWriter.append(buffer.getTableName(), buffer.getEncodedRegionName(),
        CellUtil.cloneRow(entries.get(0).getMemstoreEdits().getCells().get(0)), futures, entries,
        replay, replicaId);
    }

    @Override
    public boolean flush() throws IOException {
      // nothing much to do for now. Wait for the Writer threads to finish up
      // append()'ing the data.
      return super.flush();
    }

    @Override
    public boolean keepRegionEvent(Entry entry) {
      return true;
    }

    @Override
    public List<Path> finishWritingAndClose() throws IOException {
      finishWriting(true);
      return null;
    }

    @Override
    public Map<byte[], Long> getOutputCounts() {
      return null; // only used in tests
    }

    @Override
    public int getNumberOfRecoveredRegions() {
      return 0;
    }
  }
}
