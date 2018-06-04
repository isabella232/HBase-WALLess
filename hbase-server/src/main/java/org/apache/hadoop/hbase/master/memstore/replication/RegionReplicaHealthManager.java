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
package org.apache.hadoop.hbase.master.memstore.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RegionReplicaHealthManager extends ScheduledChore {

  private static final Log LOG = LogFactory.getLog(RegionReplicaHealthManager.class.getName());

  private HMaster master;
  private Map<RegionInfo, Long> badRegions;

  public RegionReplicaHealthManager(HMaster master, int period) {
    super("RegionReplicaHealthManagerChore", master, period);
    this.master = master;
    this.badRegions = new HashMap<>();
  }

  @Override
  protected void chore() {
    Map<RegionInfo, Long> badRegionsLocal = new HashMap<>(this.badRegions.size());
    synchronized (this) {
      badRegionsLocal.putAll(this.badRegions);
    }
    Map<ServerName, List<Pair<RegionInfo, Long>>> serverVsRegions = groupRegionsPerRS(
        badRegionsLocal);
    serverVsRegions.forEach((sn, regions) -> {
      this.master.getExecutorService().submit(new ReplicaHealthCorrectionHandler(this, this.master,
          EventType.M_REGION_REPLICA_GOOD_HEALTH_MARKER, sn, regions));
    });
  }

  public void markRegionsDown(List<RegionInfo> regions, ServerName sn) {
    LOG.info("Removing regions " + regions + " from processing as their server" + sn
        + " seems to be dead by now");
    synchronized (this) {
      regions.forEach(region -> {
        this.badRegions.remove(region);
      });
    }
  }

  private Map<ServerName, List<Pair<RegionInfo, Long>>> groupRegionsPerRS(
      Map<RegionInfo, Long> badRegionsLocal) {
    Map<ServerName, List<Pair<RegionInfo, Long>>> serverVsRegions = new HashMap<>();
    badRegionsLocal.forEach((region, ts) -> {
      ServerName rs = this.master.getAssignmentManager().getRegionStates()
          .getRegionServerOfRegion(region);
      List<Pair<RegionInfo, Long>> regions = serverVsRegions.get(rs);
      if (regions == null) {
        regions = new ArrayList<>();
        serverVsRegions.put(rs, regions);
      }
      regions.add(new Pair<RegionInfo, Long>(region, ts));
    });
    return serverVsRegions;
  }

  public void handleBadRegions(List<RegionInfo> regions, Long ts) throws IOException {
    LOG.info("Marking the replicas" + regions + " as BAD in META");
    this.master.getAssignmentManager().updateReplicaRegionHealth(regions, false);
    synchronized (this) {
      regions.forEach(region -> {
        this.badRegions.put(region, ts);
      });
    }
  }

  public void serverDown(ServerName downServer) {
    LOG.info("Server " + downServer + " got down. Removing regions in it from processing");
    synchronized (this) {
      Iterator<RegionInfo> itr = this.badRegions.keySet().iterator();
      itr.forEachRemaining(region -> {
        ServerName rs = this.master.getAssignmentManager().getRegionStates()
            .getRegionServerOfRegion(region);
        if (downServer.equals(rs)) {
          itr.remove();
        }
      });
    }
  }

  public void markRegionsGood(List<RegionInfo> regions) throws IOException {
    LOG.info("Marking the replicas" + regions + " as GOOD in META");
    this.master.getAssignmentManager().updateReplicaRegionHealth(regions, true);
    synchronized (this) {
      regions.forEach(region -> {
        this.badRegions.remove(region);
      });
    }
  }
}
