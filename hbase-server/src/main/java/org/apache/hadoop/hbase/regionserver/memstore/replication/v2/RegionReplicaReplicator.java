// TODO temp new package name
package org.apache.hadoop.hbase.regionserver.memstore.replication.v2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.regionserver.memstore.replication.CompletedFuture;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreReplicationEntry;
import org.apache.hadoop.hbase.regionserver.memstore.replication.PipelineException;

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
public class RegionReplicaReplicator {
  private final Configuration conf;
  private final HRegionInfo curRegion;
  private final int minNonPrimaryWriteReplicas;
  private volatile HRegionLocation[] regionLocations;
  private List<MemstoreReplicationEntry> entryBuffer;
  private volatile long curSeq = 0;
  private volatile long curMaxConsumedSeq = -1;
  private int replicationThreadIndex;
  // Those replicas whose health is marked as BAD already in this primary
  private Set<Integer> badReplicas;
  // This is a subset of badReplicas. We will add to below DS only after the region is marked as BAD
  // in META where as we will add to above immediately after we see a replica as out of data
  // consistency as one of the write did not reach there
  private Set<Integer> badReplicasInMeta;
  private Set<Integer> pipeline;
  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private AtomicInteger badCountToBeCommittedInMeta = new AtomicInteger(0);

  public RegionReplicaReplicator(Configuration conf, HRegionInfo currentRegion, int minWriteReplicas,
       int replicationThreadIndex) {
    this.conf = conf;
    this.curRegion = currentRegion;
    this.minNonPrimaryWriteReplicas = minWriteReplicas - 1;
    this.entryBuffer = new ArrayList<>();
    this.replicationThreadIndex = replicationThreadIndex;
    if (RegionReplicaUtil.isDefaultReplica(currentRegion)) {
      badReplicas = new HashSet<>();
      badReplicasInMeta = new HashSet<>();
    }
  }

  // Seems no way to avoid this sync
  public CompletedFuture append(MemstoreReplicationEntry entry) throws IOException {
    CompletedFuture future = new CompletedFuture();
    entry.attachFuture(future, curSeq++);
    addToBuffer(entry);
    return future;
  }

  private synchronized void addToBuffer(MemstoreReplicationEntry entry) {
    this.entryBuffer.add(entry);
  }

  /**
   * @param minSeq
   *          The min sequence number we expect to return. If that is not there (already retrieved
   *          and processed by some one else), we will return null.
   */
  public synchronized List<MemstoreReplicationEntry> pullEntries(long minSeq) {
    // TODO : some problem here.  Fix !!  This is a very imp fix. What is the issue (Anoop)
/*    if (this.curMaxConsumedSeq >= minSeq) {
      return null;
    }*/
    List<MemstoreReplicationEntry> local = this.entryBuffer;
    this.entryBuffer = new ArrayList<>();
    this.curMaxConsumedSeq = this.curSeq;
    return local;
  }

  public HRegionLocation getRegionLocation(int replicaId) {
    // TODO this location may be null also as marked by the processBadReplicas(). Reload then.
    // TODO after a new replica is added at runtime, (Table alter) and if this is a secondary
    // replica, it can so happen that we dont have this replicaId itself in the array. Handle so as
    // not to get Array Index Out of Bounds.
    return this.regionLocations[replicaId];
  }

  public int getCurRegionReplicaId() {
    return this.curRegion.getReplicaId();
  }

  public TableName getTableName() {
    return this.curRegion.getTable();
  }

  public HRegionInfo getRegionInfo() {
    return this.curRegion;
  }

  public int getReplicationThreadIndex() {
    return this.replicationThreadIndex;
  }

  private void loadRegionLocationFromMeta() throws IOException {
    // The replica id been passed have not much relevance than some checks. We will get all replica
    // loactions for this region. Not using the RegionLocation cache. We are on a fresh cluster
    // connection here. We will call this only once for a region and use that throughout unless we
    // have some replica region move. That anyway will be under our control and there is no point
    // in using cached location then.
/*    if (this.locations == null) {
      synchronized (this) {
        if (this.locations == null) {
          this.locations = RegionAdminServiceCallable.getRegionLocations(connection,
              this.curRegion.getTable(), this.curRegion.getStartKey(), false,
              this.curRegion.getReplicaId());
        }
      }
    }*/
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
        this.regionLocations[replica] = null;
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
  
  // TODO to be used
  public boolean removeFromBadReplicas(Integer replica) {
    // This should be called only when this is Primary Region
    assert RegionReplicaUtil.isDefaultReplica(curRegion);
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      this.pipeline.add(replica);
      return this.badReplicas.remove(replica);
    } finally {
      lock.unlock();
    }
  }

  // TODO to be used
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

  public List<Integer> createPipeline() throws PipelineException {
    // This should be called only when this is Primary Region
    assert RegionReplicaUtil.isDefaultReplica(curRegion);
    if (this.regionLocations == null) {
      loadRegionLocations();
    }
    Lock lock = this.lock.readLock();
    lock.lock();
    // When there are some pending updates to META for making replica region(s) as BAD, we should
    // not allow writes to continue. The pipeline wont have those replicas any way and while the
    // META is not yet updated, more writes might get committed. There is chance that this META
    // update might not happen before this primary itself going down. Then META says a real BAD
    // replica as good and that might get selected as new primary. Then we might loose some data!
    if (this.badCountToBeCommittedInMeta.get() != 0) {
      throw new PipelineException();
    }
    // Early out. Already there are not enough replicas for making the write as successful. Why to
    // continue then? 
    if (this.pipeline.size() < this.minNonPrimaryWriteReplicas) {
      throw new PipelineException();
    }
    try {
      return new ArrayList<>(this.pipeline);
    } finally {
      lock.unlock();
    }
  }

  public List<Integer> verifyPipeline(List<Integer> pipeline) throws PipelineException {
    // This should be called only when this is NOT a Primary Region
    assert !RegionReplicaUtil.isDefaultReplica(curRegion);
    if (this.regionLocations == null) {
      loadRegionLocations();
    }
    // TODO no extra verify here. We can do the case of new replicas added as part of Table alter
    // and all here.
    return pipeline;
  }

  private void loadRegionLocations() throws PipelineException {
    // update the region locations here.
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      if (this.regionLocations == null) {
        RegionLocations regionLocations;
        // Every time reload will create new connection! That may be ok. Why to retain connection
        // that too one per region
        // TODO we can reset some of the thread# related stuff in this conf for connection creation.
        // Any way we know this will be used by single thread only and just once.
        try (ClusterConnection connection = (ClusterConnection) ConnectionFactory
            .createConnection(this.conf)) {
          // never use cache here
          regionLocations = RegionAdminServiceCallable.getRegionLocations(connection,
              this.getRegionInfo().getTable(), this.getRegionInfo().getStartKey(), false,
              this.getRegionInfo().getReplicaId());
        } catch (IOException e) {
          throw new PipelineException(e);
        }
        this.regionLocations = regionLocations.getRegionLocations();
      }
      this.pipeline = new TreeSet<>();
      for (HRegionLocation loc : this.regionLocations) {
        if (loc != null && loc.getServerName() != null) {
          int replicaId = loc.getRegionInfo().getReplicaId();
          if (!RegionReplicaUtil.isDefaultReplica(replicaId)) {
            this.pipeline.add(replicaId);
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }
}
