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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicaRegionHealthProtos.RSRegionReplicaHealthChangeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicaRegionHealthProtos.RegionNameMasterProcessingTimePair;
import org.apache.hadoop.hbase.util.Pair;

@InterfaceAudience.Private
public class RegionReplicaHealthManager extends ScheduledChore {

  private static final Log LOG = LogFactory.getLog(RegionReplicaHealthManager.class.getName());

  private MasterServices masterServices;
  private Map<HRegionInfo, Long> badRegions;

  public RegionReplicaHealthManager(MasterServices master, Stoppable stoppable, int period) {
    super("RegionReplicaHealthManagerChore", stoppable, period);
    this.masterServices = master;
    this.badRegions = new HashMap<>();
  }

  @Override
  protected void chore() {
    Map<HRegionInfo, Long> badRegionsLocal = new HashMap<>(this.badRegions.size());
    synchronized (this) {
      badRegionsLocal.putAll(this.badRegions);
    }
    Map<ServerName, List<Pair<HRegionInfo, Long>>> serverVsRegions = groupRegionsPerRS(
        badRegionsLocal);
    List<Pair<HRegionInfo, Long>> regionsFromDownServers = new ArrayList<>();
    serverVsRegions.forEach((sn, regions) -> {
      try {
        AdminService.BlockingInterface rsAdmin = this.masterServices.getServerManager()
            .getRsAdmin(sn);
        LOG.info(
            "Trying to contact " + sn + " for processing of bad health replica regions " + regions);
        // TODO - What is the socket timeout value here? If this is too long, we will make the reqs
        // to other server to wait longer.
        // When this chore is in run, the next schedule comes, what to do? Just cancel?
        rsAdmin.handleBadRegions(null, buildHealthChangeReq(regions));
      } catch (RegionServerStoppedException rsse) {
        regionsFromDownServers.addAll(regions);
      } catch (IOException e) {
      } catch (ServiceException e) {
      }
    });
    // We know these regions are from a down server. So no use of repeating the RPC for them. Later
    // we will reassign these regions to other RSs and end of it, we will add them to this
    // 'badRegions' list and will process then.
    if (!regionsFromDownServers.isEmpty()) {
      LOG.info("Removing regions " + regionsFromDownServers
          + " from processing as their servers seems to be dead by now");
      synchronized (this) {
        regionsFromDownServers.forEach(region -> {
          this.badRegions.remove(region.getFirst());
        });
      }
    }
  }

  private RSRegionReplicaHealthChangeRequest buildHealthChangeReq(
      List<Pair<HRegionInfo, Long>> regions) {
    RSRegionReplicaHealthChangeRequest.Builder healthChangeReqBuilder = RSRegionReplicaHealthChangeRequest
        .newBuilder();
    RegionNameMasterProcessingTimePair.Builder builder = RegionNameMasterProcessingTimePair
        .newBuilder();
    regions.forEach(regionTsPair -> {
      builder.setEncodedRegionName(
          UnsafeByteOperations.unsafeWrap(regionTsPair.getFirst().getEncodedNameAsBytes()));
      builder.setMasterTs(regionTsPair.getSecond());
      healthChangeReqBuilder.addRegionNameProcessingTimePair(builder.build());
      builder.clear();
    });
    RSRegionReplicaHealthChangeRequest healthChangeReq = healthChangeReqBuilder.build();
    return healthChangeReq;
  }

  private Map<ServerName, List<Pair<HRegionInfo, Long>>> groupRegionsPerRS(
      Map<HRegionInfo, Long> badRegionsLocal) {
    Map<ServerName, List<Pair<HRegionInfo, Long>>> serverVsRegions = new HashMap<>();
    badRegionsLocal.forEach((region, ts) -> {
      ServerName rs = this.masterServices.getAssignmentManager().getRegionStates()
          .getRegionServerOfRegion(region);
      List<Pair<HRegionInfo, Long>> regions = serverVsRegions.get(rs);
      if (regions == null) {
        regions = new ArrayList<>();
        serverVsRegions.put(rs, regions);
      }
      regions.add(new Pair<HRegionInfo, Long>(region, ts));
    });
    return serverVsRegions;
  }

  public void handleBadRegions(List<HRegionInfo> regions, Long ts) throws IOException {
    LOG.info("Marking the replicas" + regions + " as BAD in META");
    this.masterServices.getAssignmentManager().updateReplicaRegionHealth(regions, false);
    synchronized (this) {
      regions.forEach(region -> {
        this.badRegions.put(region, ts);
      });
    }
  }

  public void serverDown(ServerName downServer) {
    LOG.info("Server " + downServer + " got down. Removing regions in it from processing");
    synchronized (this) {
      Iterator<HRegionInfo> itr = this.badRegions.keySet().iterator();
      itr.forEachRemaining(region -> {
        ServerName rs = this.masterServices.getAssignmentManager().getRegionStates()
            .getRegionServerOfRegion(region);
        if (downServer.equals(rs)) {
          itr.remove();
        }
      });
    }
  }

  public void markAsGoodRegions(List<HRegionInfo> regions) throws IOException {
    LOG.info("Marking the replicas" + regions + " as GOOD in META");
    this.masterServices.getAssignmentManager().updateReplicaRegionHealth(regions, true);
    synchronized (this) {
      regions.forEach(region -> {
        this.badRegions.remove(region);
      });
    }
  }
}
