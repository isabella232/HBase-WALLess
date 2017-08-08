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
import java.io.InterruptedIOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.FlushRegionCallable;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreReplicator.RegionReplicaReplayCallable;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;

/**
 * Helps to flush in the secondary and tertiary region whenever the primary region flushes.
 */
@InterfaceAudience.Private
public class MemstoreReplicaFlushHandler extends EventHandler {

  private static final Log LOG = LogFactory.getLog(MemstoreReplicaFlushHandler.class);

  private final ClusterConnection connection;
  private final RpcRetryingCallerFactory rpcRetryingCallerFactory;
  private final RpcControllerFactory rpcControllerFactory;
  private final int operationTimeout;
  private final HRegion region;

  public MemstoreReplicaFlushHandler(Server server, ClusterConnection connection,
      RpcRetryingCallerFactory rpcRetryingCallerFactory, RpcControllerFactory rpcControllerFactory,
      int operationTimeout, HRegion region) {
    super(server, EventType.RS_REGION_REPLICA_FLUSH);
    this.connection = connection;
    this.rpcRetryingCallerFactory = rpcRetryingCallerFactory;
    this.rpcControllerFactory = rpcControllerFactory;
    this.operationTimeout = operationTimeout;
    this.region = region;
  }

  @Override
  public void process() throws IOException {
    triggerFlushInReplicaRegions(region);
  }

  @Override
  protected void handleException(Throwable t) {
    if (t instanceof InterruptedIOException || t instanceof InterruptedException) {
      LOG.error("Caught throwable while processing event " + eventType, t);
    } else if (t instanceof RuntimeException) {
      server.abort("ServerAborting because a runtime exception was thrown", t);
    } else {
      // something fishy since we cannot flush the primary region until all retries (retries from
      // rpc times 35 trigger). We cannot close the region since there is no such mechanism to
      // close a region without master triggering it. We just abort the server for now.
      server.abort("ServerAborting because an exception was thrown", t);
    }
  }

  private int getRetriesCount(Configuration conf) {
    int numRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    if (numRetries > 10) {
      int mult = conf.getInt("hbase.client.serverside.retries.multiplier", 10);
      numRetries = numRetries / mult; // reset if HRS has multiplied this already
    }
    return numRetries;
  }

  void triggerFlushInReplicaRegions(final HRegion region) throws IOException, RuntimeException {
    long pause = connection.getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);

    int maxAttempts = getRetriesCount(connection.getConfiguration());
    RetryCounter counter = new RetryCounterFactory(maxAttempts, (int)pause).create();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to do an RPC to the region replica " + ServerRegionReplicaUtil
        .getRegionInfoForDefaultReplica(region.getRegionInfo()).getEncodedName() + " of region "
       + region.getRegionInfo().getEncodedName() + " to trigger a flush");
    }
    RegionLocations locations = null;
    boolean useCache = true;
    while (!region.isClosing() && !region.isClosed()
        && !server.isAborted() && !server.isStopped()) {
      try {
        locations = RegionReplicaReplayCallable.getRegionLocations(connection,
          region.getTableDesc().getTableName(), region.getRegionInfo().getStartKey(), useCache, 0);

        if (locations == null) {
          throw new HBaseIOException(
              "Cannot locate locations for " + region.getTableDesc().getTableName() + ", row:"
                  + Bytes.toStringBinary(region.getRegionInfo().getStartKey()));
        }
      } catch (TableNotFoundException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Not going to flush");
        }
        return;
      }
      HRegionLocation primaryLocation = locations.getDefaultRegionLocation();
      if (!Bytes.equals(primaryLocation.getRegionInfo().getEncodedNameAsBytes(),
        region.getRegionInfo().getEncodedNameAsBytes())) {
        if (useCache) {
          useCache = false;
          continue; // this will retry location lookup
        }
        LOG.info("Not calling flush as the region as the requesting region and the location is not the same");
        return;
      }
      if (locations.size() == 1) {
        return;
      }
      
      // All passed entries should belong to one region because it is coming from the EntryBuffers
      // split per region. But the regions might split and merge (unlike log recovery case).
      // TODO : Unify all these code
      FlushRegionResponse response = null;
      for (int replicaId = 0; replicaId < locations.size(); replicaId++) {
        HRegionLocation location = locations.getRegionLocation(replicaId);
        if (!RegionReplicaUtil.isDefaultReplica(replicaId)) {
          HRegionInfo regionInfo = location == null
              ? RegionReplicaUtil.getRegionInfoForReplica(
                locations.getDefaultRegionLocation().getRegionInfo(), replicaId)
              : location.getRegionInfo();
          // TODO : change param ordering
          FlushRegionCallable flushCallable =
              new FlushRegionCallable(connection, rpcControllerFactory, regionInfo, true, replicaId);

          // do not have to wait for the whole flush here, just initiate it.
          try {
            response = rpcRetryingCallerFactory.<FlushRegionResponse> newCaller()
                .callWithRetries(flushCallable, this.operationTimeout);
          } catch (IOException ex) {
            if (ex instanceof TableNotFoundException
                || connection.isTableDisabled(region.getRegionInfo().getTable())) {
              return;
            }
            throw ex;
          }

        if (response.getFlushed()) {
          // then we have to wait for seeing the flush entry. All reads will be rejected until we
          // see
          // a complete flush cycle or replay a region open event
          if (LOG.isDebugEnabled()) {
            LOG.debug("Successfully triggered a flush of primary region replica "
                + ServerRegionReplicaUtil.getRegionInfoForDefaultReplica(region.getRegionInfo())
                    .getEncodedName()
                + " of region " + region.getRegionInfo().getEncodedName()
                + " Now waiting and blocking reads until observing a full flush cycle");
          }
          break;
        } else {
          if (response.hasWroteFlushWalMarker()) {
            if (response.getWroteFlushWalMarker()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully triggered an empty flush marker(memstore empty) of primary "
                    + "region replica "
                    + ServerRegionReplicaUtil.getRegionInfoForDefaultReplica(region.getRegionInfo())
                        .getEncodedName()
                    + " of region " + region.getRegionInfo().getEncodedName() + " Now waiting and "
                    + "blocking reads until observing a flush marker");
              }
              break;
            } else {
              // somehow we were not able to get the primary to write the flush request. It may be
              // closing or already flushing. Retry flush again after some sleep.
              if (!counter.shouldRetry()) {
                throw new IOException("Cannot cause replica to flush or drop a wal marker after "
                    + "retries. Failing opening of this region replica "
                    + region.getRegionInfo().getEncodedName());
              }
            }
          } else {
            // TODO : remove these casesnothing to do. Are we dealing with an old server?
            LOG.warn(
              "Was not able to trigger a flush from primary region due to old server version? "
                  + "Continuing to open the secondary region replica: "
                  + region.getRegionInfo().getEncodedName());
            region.setReadsEnabled(true);
            break;
          }
        }
        try {
          counter.sleepUntilNextRetry();
        } catch (InterruptedException e) {
          throw new InterruptedIOException(e.getMessage());
        }
        }
      }
    }
  }

}
