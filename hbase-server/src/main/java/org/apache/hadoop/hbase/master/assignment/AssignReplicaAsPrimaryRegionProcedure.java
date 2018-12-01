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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.ReplicaToPrimaryRegionConvertOperation;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AssignReplicaRegionAsPrimaryStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class AssignReplicaAsPrimaryRegionProcedure extends AssignProcedure {

  private static final Log LOG = LogFactory.getLog(AssignReplicaAsPrimaryRegionProcedure.class);
  private RegionInfo destinationRegion;// The replica region which is going to be converted as the
                                        // primary. This in other terms that on the destination
                                        // server we are doing a partial kind of region open

  public AssignReplicaAsPrimaryRegionProcedure() {
  }

  public AssignReplicaAsPrimaryRegionProcedure(RegionInfo regionInfo, ServerName destinationServer,
      RegionInfo destinationRegion) {
    super(regionInfo, destinationServer);
    this.destinationRegion = destinationRegion;
    LOG.info("Creating assign replica procedure for the region " + regionInfo + " dest region "
        + destinationRegion + " " + destinationServer);
  }
  
  @Override
  public void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    AssignReplicaRegionAsPrimaryStateData.Builder state = AssignReplicaRegionAsPrimaryStateData
        .newBuilder().setTransitionState(getTransitionState())
        .setRegionInfo((ProtobufUtil.toRegionInfo(getRegionInfo())))
        .setTargetServer(ProtobufUtil.toServerName(this.targetServer));
    if (this.destinationRegion != null) {
      state.setDestinationRegion(ProtobufUtil.toRegionInfo(this.destinationRegion));
    }
    serializer.serialize(state.build());
  }

  @Override
  public void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    AssignReplicaRegionAsPrimaryStateData state =
        serializer.deserialize(AssignReplicaRegionAsPrimaryStateData.class);
    setTransitionState(state.getTransitionState());
    setRegionInfo(ProtobufUtil.toRegionInfo(state.getRegionInfo()));
    this.targetServer = ProtobufUtil.toServerName(state.getTargetServer());
    this.destinationRegion = ProtobufUtil.toRegionInfo(state.getDestinationRegion());
  }

  @Override
  protected boolean checkForOnlineServer(MasterProcedureEnv env, RegionStateNode regionNode) {
    // the specified targetServer is not online. So get the next replica location and try with that. This will help , but now am not doing it
    boolean newServer = super.checkForOnlineServer(env, regionNode);
    /* The case here is that.
     * 1) The procedure is alive but the master restarted during the course of the procedure. All the region servers involved in this
     * procedure are alive. In that case things will go fine.  The destinationServer and destinationRegion will all be the same. The replica can
     * still be converted to primary.
     * 2) The procedure sees that the targetServer is down. Here remember that the target server is the server where one of the replica is located.
     * So since that server is down, ideally the master will start the SCP for that server and the replica region in that server will be assigned to a new server (catch*).
     * So the primary region that is part of this procedure cannot be assigned to the server that is down. So we try to assign it to to a new server which may have
     * diff start code or a new server with new host/ip but on the same node as the previous targetServer was found.
     * (catch*) - The catch is that now the replica region that was actually alive on the target server was again reassigned to the server that came up with a new host/ip or
     * a new start code, then this call to assign the primary to that server should ideally fail. We need to handle this case by throwing error from the RS side so that
     * remote procedure itself fails. (TODO). The extension to this is that if the targetServer is not reachable we should immediately shift over to the other replica region server
     * and assign the primary to that region server.
     * 3) The other complicated case but some what an extension of #2 is that - Region servers are stopped/killed one by one and master immediately starts an SCP for those.
     * Say RS A, B and C are the server names. We stop the servers in the order C, B and A. Now by the time C is stopped a SCP is started for the region in C and the target server
     * could be A or B. Similarly when B is stopped the targetSErver for those regions could be A. Then A is also stopped. So almost all the procedures are trying to find out
     * the right region server for them to be assigned. If the region in C is a primary region then it would have created a AssignReplicaAsPrimaryRegionProc with A or B
     * as the destination. It wont succeed because A and B are down. So when they are restarted with new start code or new name we should still go with what #2 does.
    */
    if (newServer) {
      this.destinationRegion = null;
    }
    return newServer;
  }

  @Override
  protected void queueForBalance(RegionStateNode regionNode, MasterProcedureEnv env,
      boolean retain) {
    // This is basically a noop as we strictly use the destination server which we already know.
    // This is a special way of Region open where the replica of this region is already open in that
    // RS and we just have to go to that RS and mark this replica as a primary one now
    // The core assign flow will wait on the Procedure for this Region node. So just mark this ready
    // so that this wait will get passed
    // 'env.getProcedureScheduler().waitEvent(regionNode.getProcedureEvent(), this))'
    LOG.info("Start " + this + "; " + regionNode.toShortString() + "; forceNewPlan="
        + this.forceNewPlan + ", retain=" + retain);
    ProcedureEvent.wakeEvents(env.getProcedureScheduler(), regionNode.getProcedureEvent());
  }

  @Override
  public RemoteOperation remoteCallBuild(final MasterProcedureEnv env,
      final ServerName serverName) {
    assert serverName.equals(getRegionState(env).getRegionLocation());
    return new ReplicaToPrimaryRegionConvertOperation(this, getRegionInfo(),
        env.getAssignmentManager().getFavoredNodes(getRegionInfo()), false, this.destinationRegion);
  }

  protected void handleFailure(final MasterProcedureEnv env, final RegionStateNode regionNode) {
    if (incrementAndCheckMaxAttempts(env, regionNode)) {
      aborted.set(true);
    }
    Pair<RegionInfo, ServerName> pair = env.getAssignmentManager()
        .getNextReplicaRegion(getRegionInfo());
    this.destinationRegion = pair.getFirst();
    this.targetServer = pair.getSecond();
    regionNode.offline();
    // We were moved to OPENING state before dispatch. Undo. It is safe to call
    // this method because it checks for OPENING first.
    env.getAssignmentManager().undoRegionAsOpening(regionNode);
    setTransitionState(RegionTransitionState.REGION_TRANSITION_QUEUE);
  }

  @Override
  protected Procedure finishTransition(final MasterProcedureEnv env,
      final RegionStateNode regionNode) throws IOException {
    // TODO : This is not enough. We have to remove the node from ServerState in REgionStates
    // which happens only when the node is closed. Here we don't close the node.
    // Because of this if we try to balance without grouping by table we have an extra node
    // and the number of regions it is balancing is totally wrong. For now going with
    // assignments by table so that the number of regions are correct. Have to fix this
    // Ensure that 'hbase.master.loadbalance.bytable' is made true
    if (this.destinationRegion != null) {
      env.getAssignmentManager().markRegionAsOffline(this.destinationRegion);
    }
    env.getAssignmentManager().markRegionAsOpened(regionNode);
    // This success may have been after we failed open a few times. Be sure to cleanup any
    // failed open references. See #incrementAndCheckMaxAttempts and where it is called.
    env.getAssignmentManager().getRegionStates().removeFromFailedOpen(regionNode.getRegionInfo());
    // remove from the region node
    env.getAssignmentManager().getRegionStates()
        .removeFromNewRequestingServer(regionNode.getRegionInfo().getRegionName());
    // TODO : Shall we add a new state for this so that on failure this assign alone is done once
    // again
    // rather than other steps??
    // TODO : if this assign fails - we have to do some thing more to continue with the assignment
    // create a server that is not part of the replica list.
    if (this.destinationRegion != null) {
      List<ServerName> replicaServers = env.getAssignmentManager().getReplicaRegionLocations(
        RegionReplicaUtil.getRegionInfoForDefaultReplica(this.destinationRegion));
      // TODO : any other better API.
      // TODO : Ideally this should be done by the balancer. Add this to LB interface.
      if (replicaServers != null) {
        List<ServerName> servers =
            env.getMasterServices().getServerManager().createDestinationServersList();
        servers.removeAll(replicaServers);
        ServerName destinationServer =
            servers.get(env.getAssignmentManager().RANDOM.nextInt(servers.size()));
        return new AssignProcedure(this.destinationRegion, destinationServer);
      } else {
        return new AssignProcedure(this.destinationRegion, true);
      }
    }
    return null;
  }
 
  @Override
  protected void postFinish(MasterProcedureEnv env, RegionStateNode regionNode) {
    // Only for this procedure we do this because we don't want the parent to be exeecuted once
    // again.
    this.transitionState = RegionTransitionState.PRIMARY_REGION_REPLICA_SWTICH_OVER;
  }
}
