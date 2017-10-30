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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicasAreDistributed1 {

  private static final Log LOG = LogFactory.getLog(TestRegionReplicasAreDistributed1.class);

  private static final int NB_SERVERS = 4;
  private static Table table;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @BeforeClass
  public static void before() throws Exception {
    HTU.getConfiguration().setInt("hbase.master.wait.on.regionservers.mintostart", 3);

    HTU.startMiniCluster(NB_SERVERS);
    Thread.sleep(3000);
    final TableName tableName =
        TableName.valueOf(TestRegionReplicasAreDistributed1.class.getSimpleName());

    // Create table then get the single region for our new table.
    createTableDirectlyFromHTD(tableName);
  }

  private static void createTableDirectlyFromHTD(final TableName tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setRegionReplication(3);
    // create a table with 3 replication

    table = HTU.createTable(htd, new byte[][] { f }, getSplits(20),
      new Configuration(HTU.getConfiguration()));
  }

  private static byte[][] getSplits(int numRegions) {
    RegionSplitter.UniformSplit split = new RegionSplitter.UniformSplit();
    split.setFirstRow(Bytes.toBytes(0L));
    split.setLastRow(Bytes.toBytes(Long.MAX_VALUE));
    return split.split(numRegions);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HRegionServer.TEST_SKIP_REPORTING_TRANSITION = false;
    table.close();
    HTU.shutdownMiniCluster();
  }

  private HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private HRegionServer getSecondaryRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(1);
  }

  private HRegionServer getTertiaryRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(2);
  }

  @Test(timeout = 60000)
  public void testRegionReplicaConvertedToPrimary() throws Exception {
    try {
      List<Region> onlineRegions = getRS().getOnlineRegions();
      HRegionInfo region = onlineRegions.get(0).getRegionInfo();
      boolean aborted = false;
      HRegionServer secondaryReplica = null;
      HRegionServer tertiaryReplica = null;
      HRegionServer primaryReplicatoAbort = null;
      HRegionInfo primaryRegionInfo = null;
      for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
        if (!aborted) {
          for (Region r : rs.getRegionServer().getOnlineRegions(table.getName())) {
            if (r.getRegionInfo().getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID) {
              // now see how does the assignment happen
              primaryRegionInfo = r.getRegionInfo();
              primaryReplicatoAbort = rs.getRegionServer();
              // rs.getRegionServer().abort("for test");
              // aborted = true;
              break;
            }
          }
        }
      }

      for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
        for (Region r : rs.getRegionServer().getOnlineRegions(table.getName())) {
          if (RegionReplicaUtil.getRegionInfoForDefaultReplica(r.getRegionInfo())
              .equals(primaryRegionInfo)) {
            if (r.getRegionInfo().getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID + 1) {
              secondaryReplica = rs.getRegionServer();
            }
            if (r.getRegionInfo().getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID + 2) {
              tertiaryReplica = rs.getRegionServer();
            }
            if (secondaryReplica != null && tertiaryReplica != null) {
              break;
            }
          }
        }
      }
      if (primaryReplicatoAbort != null) {
        primaryReplicatoAbort.abort("for test");
      }
      Thread.sleep(4000);
      int count = 0;
      for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
        count += rs.getRegionServer().getOnlineRegions(table.getName()).size();
      }
      assertEquals("the number of regions should be 60", count + 2, 60 + 2);
      boolean foundConvertedPrimary = false;
      if (secondaryReplica != null) {
        for (Region r : secondaryReplica.getOnlineRegions(table.getName())) {
          if (r.getRegionInfo().getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID) {
            if (r.getRegionInfo().equals(primaryRegionInfo)) {
              foundConvertedPrimary = true;
            }
          } else {
            if (r.getRegionInfo().getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID + 1) {
              assertFalse(r.getRegionInfo().equals(RegionReplicaUtil.getRegionInfoForReplica(
                primaryRegionInfo, RegionReplicaUtil.DEFAULT_REPLICA_ID + 1)));
            }
          }
        }
      }
      if (!foundConvertedPrimary && tertiaryReplica != null) {
        for (Region r : tertiaryReplica.getOnlineRegions(table.getName())) {
          if (r.getRegionInfo().getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID) {
            assertFalse(foundConvertedPrimary);
            if (r.getRegionInfo().equals(primaryRegionInfo)) {
              foundConvertedPrimary = true;
            }
          } else {
            if (r.getRegionInfo().getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID + 2) {
              assertFalse(r.getRegionInfo().equals(RegionReplicaUtil.getRegionInfoForReplica(
                primaryRegionInfo, RegionReplicaUtil.DEFAULT_REPLICA_ID + 2)));
            }
          }
        }
      }
      assertTrue(foundConvertedPrimary);
    } finally {
      HTU.getAdmin().disableTable(table.getName());
      HTU.getAdmin().deleteTable(table.getName());
    }
  }

  private void checkAndAssertRegionDistribution() throws Exception {
    Collection<Region> onlineRegions = getRS().getOnlineRegionsLocalContext();
    boolean res = checkDuplicates(onlineRegions);
    assertFalse(res);
    Collection<Region> onlineRegions2 = getSecondaryRS().getOnlineRegionsLocalContext();
    res = checkDuplicates(onlineRegions2);
    assertFalse(res);
    Collection<Region> onlineRegions3 = getTertiaryRS().getOnlineRegionsLocalContext();
    checkDuplicates(onlineRegions3);
    assertFalse(res);
    int totalRegions = onlineRegions.size() + onlineRegions2.size() + onlineRegions3.size();
    // META and namespace to be added
    assertEquals("the number of regions should be 60", totalRegions, 60 + 2);
  }

  private boolean checkDuplicates(Collection<Region> onlineRegions) throws Exception {
    ArrayList<Region> copyOfRegion = new ArrayList<Region>(onlineRegions);
    for (Region region : copyOfRegion) {
      HRegionInfo regionInfo = region.getRegionInfo();
      HRegionInfo regionInfoForReplica =
          RegionReplicaUtil.getRegionInfoForDefaultReplica(regionInfo);
      int i = 0;
      for (Region actualRegion : onlineRegions) {
        if (regionInfoForReplica.equals(
          RegionReplicaUtil.getRegionInfoForDefaultReplica(actualRegion.getRegionInfo()))) {
          i++;
          if (i > 1) {
            LOG.error(
              "duplicate found " + actualRegion.getRegionInfo() + " " + region.getRegionInfo());
            assertTrue(Bytes.equals(region.getRegionInfo().getStartKey(),
              actualRegion.getRegionInfo().getStartKey()));
            assertTrue(Bytes.equals(region.getRegionInfo().getEndKey(),
              actualRegion.getRegionInfo().getEndKey()));
            return true;
          }
        }
      }
    }
    return false;
  }
}
