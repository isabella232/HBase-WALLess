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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTagsAndSeqNo;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.memstore.replication.protobuf.MemstoreReplicationProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos.ReplicateMemstoreRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos.ReplicateMemstoreResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class SimpleMemstoreReplicator implements MemstoreReplicator {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleMemstoreReplicator.class);
  private static final String MEMSTORE_REPLICATION_THREAD_COUNT = 
      "hbase.regionserver.memstore.replication.threads";
  private static final String MEMSTORE_REPLICATION_CONNECTIONS_COUNT = 
      "hbase.regionserver.memstore.replication.connections";
  private final Configuration conf;
  private ClusterConnection[] connections;
  private final int operationTimeout;
  private final ReplicationThread[] replicationThreads;
  private final long replicationTimeout;
  protected static final int DEFAULT_WAL_SYNC_TIMEOUT_MS = 5 * 60 * 1000;
  protected final RegionServerServices rs;
  private AtomicInteger threadIndex = new AtomicInteger(0);
  private AtomicInteger connIndex = new AtomicInteger(0);
  private final RpcControllerFactory rpcControllerFactory;
  private final RpcRetryingCallerFactory rpcRetryingCallerFactory;
  
  public SimpleMemstoreReplicator(Configuration conf, RegionServerServices rs) {
    this.conf = HBaseConfiguration.create(conf);
    // Adjusting the client retries number. This defaults to 31. (Multiplied by 10?)
    // The more this retries, the more latency we will have when we have some region replica write
    // fails. Adding a new config may be needed. As of now just making this to 2. And the multiplier to 1.
    this.conf.setInt("hbase.client.serverside.retries.multiplier", 1);
    this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    // We need to write the seqId also along with all cells for the memstore replication
    this.conf.set(HConstants.RPC_CODEC_CONF_KEY,
        KeyValueCodecWithTagsAndSeqNo.class.getCanonicalName());

    // TODO : Better math considering Regions count also? As per the cur parallel model, this is enough
    // use the regular RPC timeout for replica replication RPC's
    this.operationTimeout = this.conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);

    // TODO As of now using the same config and default value as for WAL sync timeout. Better to
    // have a new config.
    int numWriterThreads = this.conf.getInt(MEMSTORE_REPLICATION_THREAD_COUNT,
      HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
    int numConn = this.conf.getInt(MEMSTORE_REPLICATION_CONNECTIONS_COUNT, (numWriterThreads));
    this.replicationTimeout = this.conf.getLong("hbase.regionserver.mutations.sync.timeout",
        DEFAULT_WAL_SYNC_TIMEOUT_MS);
    this.connections = new ClusterConnection[numConn];
    for (int i = 0; i < numConn; i++) {
      try {
        this.connections[i] =
            (ClusterConnection) ConnectionFactory.createConnection(this.conf);
      } catch (IOException ex) {
        throw new RuntimeException("Exception while creating SimpleMemstoreReplicator", ex);
      }
    }
    this.rpcRetryingCallerFactory =
        RpcRetryingCallerFactory.instantiate(connections[0].getConfiguration());
    this.rpcControllerFactory =
        RpcControllerFactory.instantiate(connections[0].getConfiguration());
    this.replicationThreads = new ReplicationThread[numWriterThreads];
    for (int i = 0; i < numWriterThreads; i++) {
      this.replicationThreads[i] = new ReplicationThread();
      this.replicationThreads[i].start();
    }
    this.rs = rs;
  }

  @Override
  public ReplicateMemstoreResponse replicate(MemstoreReplicationKey memstoreReplicationKey,
      MemstoreEdits memstoreEdits, RegionReplicaCoordinator replicaCordinator, boolean metaMarkerReq)
      throws IOException {
    CompletableFuture<ReplicateMemstoreResponse> future =
        offerForReplicate(memstoreReplicationKey, memstoreEdits, replicaCordinator, metaMarkerReq);
    try {
      return future.get(replicationTimeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      LOG.error("This exception is caused by " + replicaCordinator.getRegionInfo(), e);
      throw new TimeoutIOException(e);
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("This exception is caused by " + replicaCordinator.getRegionInfo(), e);
      throw new IOException(e);
    }
  }

  private CompletableFuture<ReplicateMemstoreResponse> offerForReplicate(
      MemstoreReplicationKey memstoreReplicationKey, MemstoreEdits memstoreEdits,
      RegionReplicaCoordinator replicaCordinator, boolean metaMarkerReq) throws IOException {
    MemstoreReplicationEntry entry = new MemstoreReplicationEntry(memstoreReplicationKey,
        memstoreEdits, metaMarkerReq);
    CompletableFuture<ReplicateMemstoreResponse> future = replicaCordinator.append(entry);
    offer(replicaCordinator, entry);
    return future;
  }

  @Override
  public CompletableFuture<ReplicateMemstoreResponse> replicateAsync(
      MemstoreReplicationKey memstoreReplicationKey, MemstoreEdits memstoreEdits,
      RegionReplicaCoordinator replicaCordinator, boolean metaMarkerReq) throws IOException {
    // ideally the same should be done for both async and sync case. But it does not work so.
    return offerForReplicate(memstoreReplicationKey, memstoreEdits, replicaCordinator,
        metaMarkerReq);
  }

  @Override
  // directly waiting on this? Is it better to go with the rep threads here too???
  public ReplicateMemstoreResponse replicate(ReplicateMemstoreRequest request,  Map<byte[], List<Cell>> allCells,
      ByteBuff cellScannerBB, RegionReplicaCoordinator replicator) throws IOException {
    CompletableFuture<ReplicateMemstoreResponse> future = new CompletableFuture<>(); 
    replicate(new RequestEntryHolder(request, allCells, cellScannerBB, future), null, replicator, false, -1);
    try {
      return future.get(replicationTimeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e);
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }


  public void stop() {
    for (ReplicationThread thread : this.replicationThreads) {
      thread.stop();
    }
  }

  public void offer(RegionReplicaCoordinator replicaCordinator, MemstoreReplicationEntry entry) {
    int index = replicaCordinator.getReplicationThreadIndex();
    this.replicationThreads[index].regionQueue
        .offer(new RegionQueueEntry(replicaCordinator, entry.getSeq(), entry.isMetaMarkerReq()));
  }

  // called only when the region replicator is created
  @Override
  public int getNextReplicationThread() {
    return (threadIndex.getAndIncrement()) % this.replicationThreads.length;
  }

  /*
   * Either we pass request directly or pass replicationEntries. For replication from primary
   * region, request will be null. For other replicas, replicating to its next, the request will be
   * reused and we pass the request here. We will have the pipeline info already in the req then.
   * For primary, make up the pipeline here.
   */
  void replicate(RequestEntryHolder request, List<MemstoreReplicationEntry> replicationEntries,
      RegionReplicaCoordinator replicaCordinator, boolean specialCell, long seq) {
    int curRegionReplicaId = replicaCordinator.getCurRegionReplicaId();
    List<Pair<Integer, ServerName>> pipeline = null;
    try {
      if (request == null) {
        pipeline = replicaCordinator.createPipeline(specialCell);
      } else {
        pipeline = replicaCordinator.verifyPipeline(request.request.getReplicasList(),
          request.request.getLocationsList());
      }
    } catch (PipelineException e) {
      for (MemstoreReplicationEntry entry : replicationEntries) {
        entry.markException(e);
      }
    }
    ReplicateMemstoreResponse.Builder builder = ReplicateMemstoreResponse.newBuilder();
    builder.setReplicasCommitted(1);
    try {
      // The write pipeline for replication will always be R1 -> R2 ->.. Rn
      // When there is a failure for any node, the current replica will try with its next and so on
      // Replica ids are like 0, 1, 2...
      // TODO : If a RS goes down we assign the region to the same RS. So two replicas in same RS is
      // not right. Fix it in LB.
      if (pipeline != null) {
        for (Pair<Integer, ServerName> replica : pipeline) {
          if (replica.getFirst() <= curRegionReplicaId) continue;
          HRegionLocation nextRegionLocation = replicaCordinator.getRegionLocation(replica.getFirst());
          if (nextRegionLocation == null) {
            // This can happen. Then we will have to reload from META.
            // TODO
            LOG.info("Next region location is null. So returning for replica " + replica + " "
                + replicaCordinator.getRegionInfo() + " " + (request != null) + " "+pipeline);
            return;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Replicating from region " + replicaCordinator.getRegionLocation(curRegionReplicaId)
                + "  to the next replica " + nextRegionLocation);
          }
          // We were passing 'null' instead of the start key previously. In the latest code in
          // ConnectionImpl this was throwing NPE and not only that when there is an error
          // the location in the client cache is updated for which the start key is needed.
          // If this is not there we were getting the following msg (this was happening in previous
          // code also)
          //'Coding error, see method javadoc. row=null, tableName=tableName)';
          // Since we know the region and its start key it is ok to pass it here
          //
          ClusterConnection con = connections[replicaCordinator.getConnIndex()];
          RpcControllerFactory controllerFactory = rpcControllerFactory;
          RpcRetryingCallerFactory retryingCallerFactory = rpcRetryingCallerFactory;
          RegionReplicaReplayCallable callable = new RegionReplicaReplayCallable(con,
            controllerFactory, replicaCordinator.getTableName(), nextRegionLocation,
              nextRegionLocation.getRegion(), nextRegionLocation.getRegion().getStartKey(),
              request, replicationEntries, pipeline, specialCell);
          try {
            ReplicateMemstoreResponse response =
                retryingCallerFactory.<ReplicateMemstoreResponse> newCaller()
                    .callWithRetries(callable, operationTimeout);
            // we need this because we may have a success after some failures.
            builder.setReplicasCommitted(response.getReplicasCommitted() + 1);// Adding this write
                                                                              // itelf as success.
            // Since only primary takes the decision of marking the META as bad we need
            // to pass on this information till the primary
            for (int replicaId : response.getFailedReplicasList()) {
              builder.addFailedReplicas(replicaId);
            }
            break;// Break the inner for loop
          } catch (IOException | RuntimeException e) {
            LOG.error("The exception is " + replicaCordinator.getRegionInfo() + " " + seq, e);
            // There may be other parallel handlers also trying to write to that replica.
            builder.addFailedReplicas(replica.getFirst());
            // We should mark the future with exception only after retrying with the other Replicas
            // so that the write is successful??
          }
        }
      }
    } finally {
      markResponse(request, replicationEntries, builder.build(), replicaCordinator);
    }
  }

  public int getNextConnectionIndex() {
    return (connIndex.getAndIncrement()) % this.connections.length;
  }

  private void markResponse(RequestEntryHolder request,
      List<MemstoreReplicationEntry> replicationEntries, ReplicateMemstoreResponse response,
      RegionReplicaCoordinator replicaCordinator) {
    if (request != null) {
      request.markResponse(response);
    } else {
      for (MemstoreReplicationEntry entry : replicationEntries) {
        entry.markResponse(response);
      }
    }
  }
  private class RegionQueueEntry {
    private final RegionReplicaCoordinator replicaCordinator;
    private final long seq;
    private final boolean metaMarkerReq;

    RegionQueueEntry(RegionReplicaCoordinator replicaCordinator, long seq, boolean metaMarkerReq) {
      this.replicaCordinator = replicaCordinator;
      this.seq = seq;
      this.metaMarkerReq = metaMarkerReq;
    }
  }

  private class ReplicationThread extends HasThread {
    
    private volatile boolean closed = false;
    private final BlockingQueue<RegionQueueEntry> regionQueue;
    
    // TODO : create thread affinity here.
    public ReplicationThread() {
      regionQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
      while (!this.closed) {
        try {
          RegionQueueEntry entry = regionQueue.take();// TODO Check whether this call
                                           // will make the thread under wait
                                           // or whether consume CPU
          replicate(entry);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    private void replicate(RegionQueueEntry entry) {
      // TODO : Handle requests directly that comes for replica regions
      List<MemstoreReplicationEntry> entries = entry.replicaCordinator.pullEntries(entry.seq);
      if (entries == null || entries.isEmpty()) {
        return;
      }
      SimpleMemstoreReplicator.this.replicate(null, entries, entry.replicaCordinator,
          entry.metaMarkerReq, entry.seq);
    }
 
    public void stop() {
      this.closed = true;
      this.interrupt();
    }
  }

  private static class RequestEntryHolder {
    private ReplicateMemstoreRequest request;
    private  Map<byte[], List<Cell>> cells;
    private ByteBuff cellScannerBB;
    private CompletableFuture<ReplicateMemstoreResponse> future;

    public RequestEntryHolder(ReplicateMemstoreRequest request,  Map<byte[], List<Cell>> allCells,
        ByteBuff cellScannerBB, CompletableFuture<ReplicateMemstoreResponse> future) {
      this.request = request;
      this.cells = allCells;
      this.cellScannerBB = cellScannerBB;
      this.future = future;
    }

    public ReplicateMemstoreRequest getRequest() {
      return this.request;
    }

    public void markResponse(ReplicateMemstoreResponse response) {
      this.future.complete(response);
    }

    public  Map<byte[], List<Cell>> getCells() {
      return this.cells;
    }

    public ByteBuff getCellScannerBB() {
      return this.cellScannerBB;
    }
  }
  private static class RegionReplicaReplayCallable
      extends RegionAdminServiceCallable<ReplicateMemstoreResponse> {
    private RequestEntryHolder request;
    private final byte[] initialEncodedRegionName;
    private boolean primaryRegion = false;

    public RegionReplicaReplayCallable(ClusterConnection connection,
        RpcControllerFactory rpcControllerFactory, TableName tableName, HRegionLocation location,
        RegionInfo regionInfo, byte[] row, RequestEntryHolder request,
        List<MemstoreReplicationEntry> entries, List<Pair<Integer, ServerName>> pipeline,
        boolean specialCell) {
      super(connection, rpcControllerFactory, location, tableName, row, regionInfo.getReplicaId());
      this.initialEncodedRegionName = regionInfo.getEncodedNameAsBytes();
      if (request != null) {
        this.request = request;
      } else {
        primaryRegion = true;
        // only for primary regions we will have this
        if (entries != null && !entries.isEmpty()) {
          Pair<ReplicateMemstoreRequest,  Map<byte[], List<Cell>>> pair = MemstoreReplicationProtobufUtil
              .buildReplicateMemstoreEntryRequest(entries, initialEncodedRegionName, pipeline,
                  specialCell);
          this.request = new RequestEntryHolder(pair.getFirst(), pair.getSecond(), null, null);
        }
      }
    }

    @Override
    public ReplicateMemstoreResponse call(HBaseRpcController controller) throws Exception {
      // Check whether we should still replay this entry. If the regions are changed, or the
      // entry is not coming form the primary region, filter it out because we do not need it.
      // Regions can change because of (1) region split (2) region merge (3) table recreated
      boolean skip = false;
      if (!Bytes.equals(location.getRegionInfo().getEncodedNameAsBytes(),
        initialEncodedRegionName)) {
        skip = true;
      }
      ByteBuff cellScannerBB = request.getCellScannerBB();
      if (cellScannerBB != null) {
        controller.setCellScannerBB(cellScannerBB);
      } else {
        controller.setCellScanner(
          MemstoreReplicationProtobufUtil.getCellScannerOnCells(request.getCells()));
      }
      if (primaryRegion) {
        // already request is created. Primary wont carry the cellScannerBB reference
        return stub.replicateMemstore(controller, request.getRequest());
      } else {
        MemstoreReplicaProtos.ReplicateMemstoreRequest.Builder reqBuilder =
            MemstoreReplicaProtos.ReplicateMemstoreRequest.newBuilder();
        // This needs to be done. Otherwise we miss the actual replica pipeline details itself
        // and in the next replica the request is empty and we just don't replicate at all.
        for (int i = 0; i < request.getRequest().getReplicasCount(); i++) {
          reqBuilder.addReplicas(request.getRequest().getReplicas(i));
        }
        for (int i = 0; i < request.getRequest().getLocationsCount(); i++) {
          reqBuilder.addLocations(request.getRequest().getLocations(i));
        }
        reqBuilder.setEncodedRegionName(
          UnsafeByteOperations.unsafeWrap(location.getRegion().getEncodedNameAsBytes()));
        reqBuilder.setReplicasOffered(request.getRequest().getReplicasOffered() + 1);
        reqBuilder.setMaxSequenceId(request.getRequest().getMaxSequenceId());
        return stub.replicateMemstore(controller, reqBuilder.build());
      }
    }
  }

  @Override
  public long getReplicationTimeout() {
    return this.replicationTimeout;
  }
}
