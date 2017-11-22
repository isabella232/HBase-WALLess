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

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestScanFailoverWithReplicas {
  private static final Log LOG = LogFactory.getLog(TestScanFailoverWithReplicas.class);

  private static final int NB_SERVERS = 3;
  private static Table table;
  private static final byte[] row = "TestScanFailoverWithReplicas".getBytes();

  private static HRegionInfo hriPrimary;
  private static HRegionInfo hriSecondary;
  private static HRegionInfo hriTertiary;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @BeforeClass
  public static void before() throws Exception {
    // Reduce the hdfs block size and prefetch to trigger the file-link reopen
    // when the file is moved to archive (e.g. compaction)
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 8192);
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, 1);
    HTU.getConfiguration().setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 128 * 1024 * 1024);
    HTU.getConfiguration().set(HRegionServer.HBASE_REGIONSERVER_MEMSTORE_REPLICATOR_CLASS, "default");
    HTU.getConfiguration().setLong("hbase.client.scanner.caching", 20l);
    HTU.startMiniCluster(NB_SERVERS);
    final TableName tableName = TableName.valueOf(TestScanFailoverWithReplicas.class.getSimpleName());

    // Create table then get the single region for our new table.
    createTableDirectlyFromHTD(tableName);
  }

  private static void createTableDirectlyFromHTD(final TableName tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setRegionReplication(3);
    // create a table with 3 replication
    table = HTU.createTable(htd, new byte[][] { f }, (byte[][]) null,
      new Configuration(HTU.getConfiguration()));
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
  
  @Test(timeout = 300000)
  public void testScanFailOver() throws Exception {
    // this will load table.
    HTU.loadNumericRows(table, f, 0, 1000);
    // start a scan on the primary. Ensure the scan has multiple RPCs.
    Scan s = new Scan();
    s.setMaxResultSize(10l);
    ResultScanner scanner = table.getScanner(s);
    Iterator<Result> iterator = scanner.iterator();
    boolean firstRpcDone = false;
    boolean abort = false;
    while(iterator.hasNext()) {
      Result next = iterator.next();
      System.out.println("The cell is "+next);
      firstRpcDone = true;
      if(firstRpcDone) {
        // abort the primary and continue with the next call
        if (!abort) {
          abortPrimary();
          Thread.sleep(6000);
        }
        abort = true;
      }
      // this next call should be o
    }
    
  }
  
  private void abortPrimary() {
    for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      for (Region r : rs.getRegionServer().getOnlineRegions(table.getName())) {
        if (r.getRegionInfo().getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID) {
          // now see how does the assignment happen
          rs.getRegionServer().abort("for test");
          // aborted = true;
          break;
        }
      }
    }
  }
}
