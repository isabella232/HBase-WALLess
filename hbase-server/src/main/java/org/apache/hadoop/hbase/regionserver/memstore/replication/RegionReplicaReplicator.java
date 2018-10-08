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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl.WriteEntry;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos.ReplicateMemstoreResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

// This is a per Region instance 
//TODO better name
/**
 * Notes : How the different data structures like regionLocations, badReplicas, pipeline work
 * altogether. regionLocations - Keep the replica Locations in an replicaId indexed array.
 *
 * badReplicas - This is used by primary region alone. In other replica regions, this will be a
 * never used data structure. When a write to one of the replica is failed, the primary will mark it
 * as BAD and also ask HM to mark the same in the META table. Even before asking the HM, that
 * replica will be added to this set. Also when the down replicas are coming back, it would ask the
 * primary to do flush. Then itself it will be removed from badRepicas. This Set is been used for
 * the pipeline creation. Once the BAD replica comes back and asks for flush, we have to make the
 * write to go to those regions. The flush would have flushed cells till then only. The replica will
 * be marked back to GOOD in META only after the flush is finished and the replica updated its files
 * list so that it is having all data upto date. In other words, the status in META denotes which
 * all replicas are having upto date data and this Set just tells primary that which all replicas to
 * be considered for replicating data.
 *
 * badReplicasInMeta - This includes the replicas which are actually marked as BAD in META. This is
 * also a primary region side only Set.
 *
 * pipeline - This includes the indices of all active replicas at any moment, in the order of its
 * replica id. The replication of memstore writes to happen to all these. This is also a primary
 * only Set. The pipeline info will be passed the PB requests to other replicas. So each of the
 * replica use that info in the request to decide to replicate to which next replica from it.
 *
 * badCountToBeCommittedInMeta - This says whether there are some BAD replicas still which are not
 * yet marked as BAD in META. When we have such cases, all the writes during that period will be
 * immediately failed. (Will be retried from client as the Exception is not of
 * DoNotRetryIOException) This is because of the following reason. The src of truth abt the health
 * is at META. If we allow more writes when we dont replicate to a replica (as that is BAD as per
 * badReplicas set) and make them success(There may be enough number of min replicas still), we are
 * not having correct status of upto date data in META. Before making the META entry, the HM may
 * die. Also then another HM comes up and then this primary die. Still the META do not say the real
 * BAD replica as BAD. So it can so happen that this got promoted as new primary and we will have
 * data loss!
 *
 * regionLocations - Holds the location for each of the replica regions. This is in use in every
 * replica regions. For the first time, this is loaded from META read. Later when any region
 * decides replica(s) as failed (as it could not replicate to there), it calls processBadReplicas
 * and that will reset the corresponding entry in regionLocations as null. Pls see only primary will
 * change the pipeline. Later when region join back the primary, it will change the pipeline to
 * include this also and pass that to other replicas also in pipeline info. When the replication
 * process can not see the corresponding location in regionLocations for the given replica id, it
 * will reload it from META.
 */
@InterfaceAudience.Private
public class RegionReplicaReplicator {
  private static final Log LOG = LogFactory.getLog(RegionReplicaReplicator.class);
  private static final long UNSET = -1L;
  private final Configuration conf;
  private RegionInfo curRegion;
  private final int minNonPrimaryWriteReplicas;
  private volatile HRegionLocation[] regionLocations;
  private volatile ArrayList<MemstoreReplicationEntry> entryBuffer = new ArrayList<>();
  private volatile long nextSeq = 1;
  private volatile long curMaxConsumedSeq = 0;
  private int replicationThreadIndex;
  // Those replicas whose health is marked as BAD already in this primary
  private Set<Integer> badReplicas;
  // This is a subset of badReplicas. We will add to below DS only after the region is marked as BAD
  // in META where as we will add to above immediately after we see a replica as out of data
  // consistency as one of the write did not reach there
  private Set<Integer> badReplicasInMeta;
  private Map<Integer, Pair<ServerName, Boolean>> pipeline;
  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private AtomicInteger badCountToBeCommittedInMeta = new AtomicInteger(0);
  private MultiVersionConcurrencyControl mvcc;
  private volatile long badReplicaInProgressTs = UNSET;
  private int tableReplication;

  private NavigableMap<byte[], RegionReplicaStoreCordinator> storeCordinators = new TreeMap<>(
      Bytes.BYTES_COMPARATOR);

  public RegionReplicaReplicator(Configuration conf, RegionInfo currentRegion,
      MultiVersionConcurrencyControl mvcc, Set<byte[]> families, int minWriteReplicas,
      int replicationThreadIndex, int tableReplication) {
    this.conf = conf;
    this.curRegion = currentRegion;
    this.mvcc = mvcc;
    this.minNonPrimaryWriteReplicas = minWriteReplicas - 1;
    this.tableReplication = tableReplication;
    this.replicationThreadIndex = replicationThreadIndex;
    if (RegionReplicaUtil.isDefaultReplica(this.curRegion)) {
      badReplicas = new HashSet<>();
      badReplicasInMeta = new HashSet<>();
    }
    for (byte[] family : families) {
      this.storeCordinators.put(family, new RegionReplicaStoreCordinator());
    }
  }

  public RegionReplicaStoreCordinator getStoreCordinator(byte[] store) {
    return this.storeCordinators.get(store);
  }

  public CompletableFuture<ReplicateMemstoreResponse> append(MemstoreReplicationEntry entry)
      throws IOException {
    CompletableFuture<ReplicateMemstoreResponse> future = new CompletableFuture<>();
    // Seems no way to avoid this sync
    synchronized (this) {
      // begin the mvcc here if not yet
      if (entry.getMemstoreReplicationKey().getWriteEntry() == null) {
        WriteEntry we = this.mvcc.begin();
        // attach the seqId here. Ensures strict order then
        entry.getMemstoreReplicationKey().setWriteEntry(we);
      }
      entry.attachFuture(future, nextSeq++);
      this.entryBuffer.add(entry);
    }
    return future;
  }

  /**
   * @param minSeq
   *          The min sequence number we expect to return. If that is not there (already retrieved
   *          and processed by some one else), we will return null.
   */
  public synchronized List<MemstoreReplicationEntry> pullEntries(long minSeq) {
    // TODO : some problem here.  Fix !!  This is a very imp fix. What is the issue (Anoop)
    if (this.curMaxConsumedSeq >= minSeq) {
      return null;
    }
    ArrayList<MemstoreReplicationEntry> local = this.entryBuffer;
    ArrayList<MemstoreReplicationEntry> toReturn = local;
    this.entryBuffer = new ArrayList<>();
    long maxConsumedSeq = 0;
    int entryCount = 0;
    for (MemstoreReplicationEntry entry : local) {
      if (entry.isMetaMarkerReq()) {
        toReturn = new ArrayList<MemstoreReplicationEntry>();
        if (entryCount == 0) {
          toReturn.add(entry);
          this.entryBuffer.addAll(local.subList(1, local.size()));
          maxConsumedSeq = entry.getSeq();
        } else {
          toReturn.addAll(local.subList(0, entryCount));
          this.entryBuffer.addAll(local.subList(entryCount, local.size()));
          maxConsumedSeq = entry.getSeq() - 1;
        }
        break;
      }
      entryCount++;
      maxConsumedSeq = entry.getSeq();
    }
    this.curMaxConsumedSeq = maxConsumedSeq;
    return toReturn;
  }

  public HRegionLocation getRegionLocation(int replicaId) {
    // TODO this location may be null also as marked by the processBadReplicas(). Reload then.
    // TODO after a new replica is added at runtime, (Table alter) and if this is a secondary
    // replica, it can so happen that we dont have this replicaId itself in the array. Handle so as
    // not to get Array Index Out of Bounds.
    for (HRegionLocation loc : this.regionLocations) {
      if (loc != null && loc.getRegion().getReplicaId() == replicaId) {
        return loc;
      }
    }
    return null;
  }

  public int getCurRegionReplicaId() {
    return this.curRegion.getReplicaId();
  }

  public TableName getTableName() {
    return this.curRegion.getTable();
  }

  public RegionInfo getRegionInfo() {
    return this.curRegion;
  }

  public int getReplicationThreadIndex() {
    return this.replicationThreadIndex;
  }

  public List<Integer> processBadReplicas(List<Integer> replicas) {
    List<Integer> addedReplicas = new ArrayList<>(replicas.size());
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      if (RegionReplicaUtil.isDefaultReplica(curRegion)) {
        for (Integer replica : replicas) {
          if (!this.badReplicas.contains(replica)) {
            addedReplicas.add(replica);
            this.badReplicas.add(replica);
            this.pipeline.remove(replica);
          }
        }
        this.badCountToBeCommittedInMeta.addAndGet(addedReplicas.size());
      }
      // For any of the region replica, we have to update the RegionLocations. These passed replicas
      // are any way treated as BAD from now on. Later when we mark again as good, we might reload
      // the location
      for (Integer replica : replicas) {
        for (HRegionLocation loc : this.regionLocations) {
          if (loc.getRegion().getReplicaId() == replica) {
            loc = null;
            break;
          }
        }
      }
    } finally {
      lock.unlock();
    }
    return addedReplicas;
  }

  // TODO to have an API to add a new Replica into the list. This can happen when table is altered.
  // The newly opened Region will ask the primary to flush so as to make itself up to date. Primary
  // has to find out that this is a fresh replica and add here.

  /**
   * @param replicas
   * @return True if no more BAD replicas are yet to be added to META.
   */
  public void onReplicasBadInMeta(List<Integer> replicas) {
    // This should be called only when this is Primary Region
    assert RegionReplicaUtil.isDefaultReplica(curRegion);
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      for (Integer replica : replicas) {
        this.badReplicasInMeta.add(replica);
      }
      this.badCountToBeCommittedInMeta.addAndGet(-1 * replicas.size());
    } finally {
      lock.unlock();
    }
  }

  public boolean isReplicasBadInMeta(List<Integer> replicas) {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      for (Integer replica : replicas) {
        if (!this.badReplicasInMeta.contains(replica)) return false;
      }
      return true;
    } finally {
      lock.unlock();
    }
  }
  
  public boolean removeFromBadReplicas(Integer replica) throws PipelineException {
    // This should be called only when this is Primary Region
    assert RegionReplicaUtil.isDefaultReplica(curRegion);
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      // when a replica region opens it makes a call for primary region flush.
      // there is no pipeline at that point of time. This Null checks helps avoid NPE
      if (pipeline != null) {
        Pair<ServerName, Boolean> pair = this.pipeline.get(replica);
        // update the bad replica to good. so that on next pipeline only this new updated location is obtained.
        this.pipeline.put(replica, new Pair<ServerName, Boolean>(pair.getFirst(), true));
        for (HRegionLocation loc : this.regionLocations) {
          if (loc != null && loc.getRegion().getReplicaId() == replica) {
            loc.setState(true);
          }
        }
        return this.badReplicas.remove(replica);
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  public boolean removeFromBadReplicasInMeta(Integer replica) {
    // This should be called only when this is Primary Region
    assert RegionReplicaUtil.isDefaultReplica(curRegion);
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      return this.badReplicasInMeta.remove(replica);
    } finally {
      lock.unlock();
    }
  }

  // TODO - We should not give back any replicas for which state in META is still bad. There is a
  // time gap btw the Good state set here and in META. So that extra check is needed. Once we get
  // the flush request from a replica (after it is up again), then itself we will add that replica
  // into the pipeline. This is not yet ready for the reads. State is META is any way BAD still
  public List<Integer> getCurrentPiplineForReads() {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      if (pipeline != null) {
        return new ArrayList<>(this.pipeline.keySet());
      } else {
        return null;
      }
    } finally {
      lock.unlock();
    }
  }
  
  public List<Pair<Integer, ServerName>> createPipeline(boolean specialCell) throws PipelineException {
    // This should be called only when this is Primary Region
    assert RegionReplicaUtil.isDefaultReplica(curRegion);
    assert regionLocations != null;
    Lock lock = this.lock.readLock();
    lock.lock();
    // When there are some pending updates to META for making replica region(s) as BAD, we should
    // not allow writes to continue. The pipeline wont have those replicas any way and while the
    // META is not yet updated, more writes might get committed. There is chance that this META
    // update might not happen before this primary itself going down. Then META says a real BAD
    // replica as good and that might get selected as new primary. Then we might loose some data!
    // How ever still there is a chance that before this count is updated we will have pending writes already
    // in process. That will still go on. So any temp glitch will make writes successful.
    if (this.badCountToBeCommittedInMeta.get() != 0) {
      throw new PipelineException();
    }
    // Early out. Already there are not enough replicas for making the write as successful. Why to
    // continue then? 
    if(pipeline == null) {
      // This happens only for the system tables. Ideally should solve in the caller place.
      // Just leaving this for now. Because for META and namespace table we should still mark
      // the special cells in WAL and handle it. That is yet to be done.
      LOG.info("The pipeline is null "+specialCell+" "+this.curRegion);
      return null;
    }
    if (this.pipeline.size() < this.minNonPrimaryWriteReplicas) {
      throw new PipelineException();
    }
    try {
      List<Pair<Integer, ServerName>> locPipeline = new ArrayList<Pair<Integer, ServerName>>();
      for (Entry<Integer, Pair<ServerName, Boolean>> pipe : this.pipeline.entrySet()) {
        if (specialCell) {
          // don't bother about the state. Just pass on the location for the pipeline creation.
          locPipeline.add(new Pair<Integer, ServerName>(pipe.getKey(), pipe.getValue().getFirst()));
        } else {
          // pass only those locations which are in GOOD state.
          if (pipe.getValue().getSecond()) {
            locPipeline
                .add(new Pair<Integer, ServerName>(pipe.getKey(), pipe.getValue().getFirst()));
          }
        }
      }
      if (!specialCell && (locPipeline.size() < this.minNonPrimaryWriteReplicas)) {
        throw new PipelineException("Not enough good replicas found.");
      }
      LOG.debug("The pipeline to be used " + locPipeline.size() + " " + locPipeline + " "
          + this.getRegionInfo());
      return locPipeline;
    } finally {
      lock.unlock();
    }
  }

  public List<Pair<Integer, ServerName>> verifyPipeline(List<Integer> requestPipeline,
      List<org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ServerName> replicaLocations)
      throws PipelineException {
    // This should be called only when this is NOT a Primary Region
    assert !RegionReplicaUtil.isDefaultReplica(curRegion);
    // if (this.regionLocations == null || !allRegionLocationsLoaded(requestPipeline)) {
    if(replicaLocations != null && !replicaLocations.isEmpty()) {
      if(requestPipeline.size() != replicaLocations.size()) {
        // in this case try loading the region locations.
        // this should not happen
        //getRegionLocations();
        LOG.error("This should not happen at all ");
      }
    }
    if(replicaLocations != null && !replicaLocations.isEmpty()) {
      regionLocations = new HRegionLocation[replicaLocations.size()];
      int i = 0;
      for(int replicaId : requestPipeline) {
        if(!RegionReplicaUtil.isDefaultReplica(replicaId)) {
          if (replicaId == curRegion.getReplicaId()) {
            regionLocations[i] =
                new HRegionLocation(curRegion, ProtobufUtil.toServerName(replicaLocations.get(i)));
          } else {
            regionLocations[i] =
                new HRegionLocation(RegionReplicaUtil.getRegionInfoForReplica(curRegion, replicaId),
                    ProtobufUtil.toServerName(replicaLocations.get(i)));
          }
          i++;
        }
      }
    }
    if (regionLocations != null) {
      // TODO : Avoid double iteration??
      List<Pair<Integer, ServerName>> pipeline = new ArrayList<Pair<Integer, ServerName>>();
      for (HRegionLocation loc : regionLocations) {
        // If the regionLocations is available then it means we are processing the flush cell (special cell).
        // In that case just create the pipeline with all the region locations.
        if (replicaLocations != null && !replicaLocations.isEmpty()) {
          pipeline.add(
            new Pair<Integer, ServerName>(loc.getRegion().getReplicaId(), loc.getServerName()));
        } else {
          // In a normal pipeline case just ensure that we create the pipeline based on what
          // the primary has said.
          if (requestPipeline.contains(loc.getRegion().getReplicaId())) {
            // only those in the pipeline will be added here
            pipeline.add(
              new Pair<Integer, ServerName>(loc.getRegion().getReplicaId(), loc.getServerName()));
          }
        }
      }
      return pipeline;
    }
    // TODO no extra verify here. We can do the case of new replicas added as part of Table alter
    // and all here.
    return null;
  }

  // Compares the region location and pipeline and ensures that the pipeline and regoin location are the same.
  // this is needed on region open sequence where the region locations could be updated even before the replicas
  // are opened
  // TODO : Remove it after checking all cases
  private boolean allRegionLocationsLoaded(List<Integer> requestPipeline) {
    // make this more easier and avoid these loops. Better to have one more ds having the non default replicas
    // fetched as part of region locations
    if (regionLocations != null) {
      for (HRegionLocation loc : this.regionLocations) {
        if (loc != null && loc.getServerName() != null) {
          int replicaId = loc.getRegionInfo().getReplicaId();
          if ((!RegionReplicaUtil.isDefaultReplica(replicaId))
              && (!requestPipeline.contains(replicaId))) {
            LOG.info("The requested pipeline " + requestPipeline + " does not have the replica id "
                + replicaId);
            resetRegionLocations();
            return false;
          }
        }
      }
    }
    // check both ways
    for (int requestReplicaId : requestPipeline) {
      boolean found = false;
      if (regionLocations != null) {
        for (HRegionLocation loc : this.regionLocations) {
          if (loc != null && loc.getServerName() != null) {
            int replicaId = loc.getRegionInfo().getReplicaId();
            if ((!RegionReplicaUtil.isDefaultReplica(replicaId)) && requestReplicaId == replicaId) {
              found = true;
              break;
            }
          }
        }
      }
      if (!found) {
        LOG.info("The region location pipeline does not have the requested replica id "
            + requestReplicaId);
        resetRegionLocations();
        return false;
      }
    }
    return true;
  }

  private void resetRegionLocations() {
    if (regionLocations == null) {
      return;
    }
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      this.regionLocations = null;
    } finally {
      lock.unlock();
    }
  }

/*  private void loadRegionLocations(List<Integer> requestPipeline) throws PipelineException {
    // update the region locations here.
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      if (this.regionLocations == null) {
        getRegionLocations();
        if(regionLocations == null) {
          return;
        }
      }
      this.pipeline = new TreeMap<Integer, ServerName>();
      // this is the pipeline to be used by the primary
      for (HRegionLocation loc : this.regionLocations) {
        if (loc != null && loc.getServerName() != null) {
          int replicaId = loc.getRegionInfo().getReplicaId();
          if (!RegionReplicaUtil.isDefaultReplica(replicaId)) {
            this.pipeline.put(replicaId, loc.isGood());
          }
        }
      }
      LOG.debug("The pipeline created has " + this.pipeline.size() + " " + pipeline + " "
          + this.getRegionInfo());
    } finally {
      lock.unlock();
    }
  }*/

  private void getRegionLocations() throws PipelineException {
    RegionLocations regionLocations;
    // Every time reload will create new connection! That may be ok. Why to retain connection
    // that too one per region
    // TODO we can reset some of the thread# related stuff in this conf for connection creation.
    // Any way we know this will be used by single thread only and just once.
    try (ClusterConnection connection =
        (ClusterConnection) ConnectionFactory.createConnection(this.conf)) {
      regionLocations =
          RegionAdminServiceCallable.getRegionLocations(connection, this.getRegionInfo().getTable(),
            this.getRegionInfo().getStartKey(), false, this.getRegionInfo().getReplicaId());
    } catch (IOException e) {
      throw new PipelineException(e);
    }
    this.regionLocations = regionLocations.getRegionLocations();
  }

  public void convertAsPrimaryRegion(RegionInfo primaryRegion) {
    Lock lock = this.lock.writeLock();
    lock.lock();
    assert !(RegionReplicaUtil.isDefaultReplica(this.curRegion));
    assert RegionReplicaUtil.isDefaultReplica(primaryRegion);
    try {
      this.curRegion = primaryRegion;
      this.badReplicas = new HashSet<>();
      this.badReplicasInMeta = new HashSet<>();
      this.regionLocations = null;// Lets reload location from META. Any way we have done changes to
                                  // the region replica ids.
    } finally {
      lock.unlock();
    }
  }

  public synchronized void finishBadHealthProcessing() {
    assert !(RegionReplicaUtil.isDefaultReplica(this.curRegion));
    this.badReplicaInProgressTs = UNSET;
  }

  public synchronized boolean shouldProcessBadStatus(long ts) {
    assert !(RegionReplicaUtil.isDefaultReplica(this.curRegion));
    if (ts > this.badReplicaInProgressTs) {
      this.badReplicaInProgressTs = ts;
      return true;
    }
    return false;
  }

  /**
   * This method is called when the replica triggers a flush on the primary.
   * On the primary region we invovke this API passing the replica region's location(that triggered
   * the flush). Because if we try to read the META this replica region will have BAD health.
   * So we directly use this region location to update our pipeline.
   * @param replicaRegionLocation
   * @throws PipelineException
   */
  public void updateRegionLocations(HRegionLocation replicaRegionLocation) throws PipelineException {
    Lock lock = this.lock.writeLock();
    lock.lock();
    // Now take the case where a region is newly opened. In create table case.
    // we may have one of the replica triggering a flush on the primary. In that case
    // create the pipeline with the replica location that is passed over here.
    // If we can get the location from the META just try using it or update it.
    // In case of a replica region move- even in that case we will still have the regionlocation
    // but we can just update the replica location with the new location passed to us.
    try {
      if (regionLocations == null) {
        // this will update the region location
        // When can we have regionLocation as null? It can happen when there were no writes
        // to a region and its replicas but due to cluster fail over this new replica became
        // a primary.
        LOG.debug("The region locations for the primary region was actually null");
        getRegionLocations();
      }
      if(regionLocations != null && regionLocations.length != tableReplication) {
        LOG.debug("Enought region locations not found "+regionLocations.length);
        HRegionLocation tmp[] = new HRegionLocation[tableReplication];
        for(HRegionLocation loc : this.regionLocations) {
          if(loc != null) {
            tmp[loc.getRegion().getReplicaId()] = loc;
          }
        }
        // updated the region locations
        this.regionLocations = tmp;
      }
      // TODO : Will the region location array be even less than the number of replicas??
      // this logic needs some better way.
      if (regionLocations != null) {
        boolean requestRepReplicaFound = false;
        for (int i = 0; i < regionLocations.length; i++) {
          if (regionLocations[i] != null) {
            if (regionLocations[i].getRegion().getReplicaId() == replicaRegionLocation.getRegion()
                .getReplicaId()) {
              // TODO : check if this case will still happen.
              // update the region location
              regionLocations[i] = replicaRegionLocation;
              requestRepReplicaFound = true;
              break;
            }
          }
        }
        if (!requestRepReplicaFound) {
          if (regionLocations[replicaRegionLocation.getRegion().getReplicaId()] == null) {
            regionLocations[replicaRegionLocation.getRegion().getReplicaId()] =
                replicaRegionLocation;
          }
        }
        // update the pipeline also here. Every time the pipeline is updated here.
        this.pipeline = new TreeMap<>();
        for (HRegionLocation loc : this.regionLocations) {
          if (loc != null && loc.getServerName() != null) {
            int replicaId = loc.getRegion().getReplicaId();
            if (!RegionReplicaUtil.isDefaultReplica(replicaId)) {
              this.pipeline.put(replicaId,
                new Pair<ServerName, Boolean>(loc.getServerName(), loc.isGood()));
            }
          }
        }
        LOG.debug("The pipeline created has " + this.pipeline.size() + " " + pipeline + " "
            + this.getRegionInfo());
      }
    } finally {
      lock.unlock();
    }
  }
}
