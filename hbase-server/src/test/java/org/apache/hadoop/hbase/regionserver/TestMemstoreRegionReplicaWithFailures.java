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

import static org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster.closeRegion;
import static org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster.openRegion;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.TestRegionReplicasWith3Replicas.OpenedIn;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestMemstoreRegionReplicaWithFailures {
  private static final Log LOG = LogFactory.getLog(TestMemstoreRegionReplicaWithFailures.class);

  private static final int NB_SERVERS = 3;
  private static Table table;
  private static final byte[] row = "TestMemstoreRegionReplicaWithFailures".getBytes();

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

    HTU.startMiniCluster(NB_SERVERS);
    final TableName tableName = TableName.valueOf(TestRegionReplicasWith3Replicas.class.getSimpleName());

    // Create table then get the single region for our new table.
    table = HTU.createTable(tableName, f);

    try (RegionLocator locator = HTU.getConnection().getRegionLocator(tableName)) {
      hriPrimary = locator.getRegionLocation(row, false).getRegionInfo();
    }

    // mock a secondary region info to open
    hriSecondary = new HRegionInfo(hriPrimary.getTable(), hriPrimary.getStartKey(),
        hriPrimary.getEndKey(), hriPrimary.isSplit(), hriPrimary.getRegionId(), 1);
    // mock a tertiary region info to open
    hriTertiary = new HRegionInfo(hriPrimary.getTable(), hriPrimary.getStartKey(),
        hriPrimary.getEndKey(), hriPrimary.isSplit(), hriPrimary.getRegionId(), 2);

    // No master
    TestRegionServerNoMaster.stopMasterAndAssignMeta(HTU);
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

  @Test(timeout = 6000000)
  public void testFailWriteOnTertiary() throws Exception {
    Pair<OpenedIn, OpenedIn> pair = null;
    OpenedIn tertiaryOpenedIn = null;
    HRegion tertiaryRegion = null;
    try {
      pair = openSecondary();
      tertiaryOpenedIn = openTertiary(pair);
      tertiaryRegion = (HRegion)getTertiaryRegion(tertiaryOpenedIn);
      tertiaryRegion.throwErrorOnMemstoreReplay(true);
      byte[] data = Bytes.toBytes(String.valueOf(100));
      Put put = new Put(data);
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(f, null, data);
      table.put(put);
      // flush so that region replica can read
      Region primaryRegion = getPrimaryRegion(pair);

      // just sleeping to see if the value is visible
      // try directly Get against region replica
      byte[] row = Bytes.toBytes(String.valueOf(100));
      Get get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(1);
      Result result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } finally {
      tertiaryRegion.throwErrorOnMemstoreReplay(false);
      HTU.deleteNumericRows(table, HConstants.CATALOG_FAMILY, 100, 101);
      closeSecondary(pair.getSecond());
      closeTertiary(tertiaryOpenedIn);
    }
  }

  @Test(timeout = 6000000)
  public void testFailWriteOnSecondary() throws Exception {
    Pair<OpenedIn, OpenedIn> pair = null;
    OpenedIn tertiaryOpenedIn = null;
    HRegion secondarRegion = null;
    try {
      pair = openSecondary();
      tertiaryOpenedIn = openTertiary(pair);
      secondarRegion = (HRegion)getSecondaryRegion(pair);
      secondarRegion.throwErrorOnMemstoreReplay(true);
      byte[] data = Bytes.toBytes(String.valueOf(100));
      Put put = new Put(data);
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(f, null, data);
      table.put(put);
      // flush so that region replica can read
      Region primaryRegion = getPrimaryRegion(pair);

      // just sleeping to see if the value is visible
      // try directly Get against region replica
      byte[] row = Bytes.toBytes(String.valueOf(100));
      Get get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(2);
      Result result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } finally {
      secondarRegion.throwErrorOnMemstoreReplay(false);
      HTU.deleteNumericRows(table, HConstants.CATALOG_FAMILY, 100, 101);
      closeSecondary(pair.getSecond());
      closeTertiary(tertiaryOpenedIn);
    }
  }

  @Test(timeout = 6000000)
  public void testFailWriteOnSecondaryWithTimeline() throws Exception {
    Pair<OpenedIn, OpenedIn> pair = null;
    OpenedIn tertiaryOpenedIn = null;
    HRegion secondaryRegion = null;
    HRegion primaryRegion = null;
    try {
      pair = openSecondary();
      tertiaryOpenedIn = openTertiary(pair);
      secondaryRegion = (HRegion)getSecondaryRegion(pair);
      secondaryRegion.throwErrorOnMemstoreReplay(true);
      byte[] data = Bytes.toBytes(String.valueOf(100));
      Put put = new Put(data);
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(f, null, data);
      table.put(put);
      // flush so that region replica can read
      primaryRegion = (HRegion) getPrimaryRegion(pair);
      primaryRegion.throwErrorOnScan(true);
      // just sleeping to see if the value is visible
      // try directly Get against region replica
      byte[] row = Bytes.toBytes(String.valueOf(100));
      Get get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(2);
      Result result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
      
      row = Bytes.toBytes(String.valueOf(100));
      get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      result = table.get(get);
      // this should some how get a result from tertiary. Lets see
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } finally {
      secondaryRegion.throwErrorOnMemstoreReplay(false);
      primaryRegion.throwErrorOnScan(false);
      HTU.deleteNumericRows(table, HConstants.CATALOG_FAMILY, 100, 101);
      closeSecondary(pair.getSecond());
      closeTertiary(tertiaryOpenedIn);
      // TODO : fix - test case not shutting down
    }
  }

  private OpenedIn openTertiary(Pair<OpenedIn, OpenedIn> pair) throws Exception {
    // we know the combination
    OpenedIn tertiaryOpenedIn = null;
    switch (pair.getFirst()) {
    case PRIMARY:
      LOG.debug("Opening the tertiary in tertiary region server");
      openRegion(HTU, getTertiaryRS(), hriTertiary);
      tertiaryOpenedIn = OpenedIn.TERTIARY;
      break;
    case SECONDARY:
      LOG.debug("Opening the tertiary in primary region server");
      openRegion(HTU, getRS(), hriTertiary);
      tertiaryOpenedIn = OpenedIn.PRIMARY;
      break;
    case TERTIARY:
      LOG.debug("Opening the tertiary in secondary region server");
      openRegion(HTU, getSecondaryRS(), hriTertiary);
      tertiaryOpenedIn = OpenedIn.SECONDARY;
      break;
    }
    return tertiaryOpenedIn;
  }
  
  private void closeTertiary(OpenedIn tertiaryOpenedIn) throws Exception {
    switch (tertiaryOpenedIn) {
    case PRIMARY:
      closeRegion(HTU, getRS(), hriTertiary);
      break;
    case SECONDARY:
      closeRegion(HTU, getSecondaryRS(), hriTertiary);
      break;
    case TERTIARY:
      closeRegion(HTU, getTertiaryRS(), hriTertiary);
      break;
    }
  }

  private void closeSecondary(OpenedIn openedIn) throws Exception {
    switch (openedIn) {
    case PRIMARY:
      closeRegion(HTU, getRS(), hriSecondary);
      break;
    case SECONDARY:
      closeRegion(HTU, getSecondaryRS(), hriSecondary);
      break;
    case TERTIARY:
      closeRegion(HTU, getTertiaryRS(), hriSecondary);
      break;
    }
  }
  
  private Region getPrimaryRegion(Pair<OpenedIn, OpenedIn> pair) throws NotServingRegionException {
    Region region = null;
    switch (pair.getFirst()) {
    case PRIMARY:
      // If secondary is opened in Primary
      region = getRS().getRegionByEncodedName(hriPrimary.getEncodedName());
      break;
    case SECONDARY:
      region = getSecondaryRS().getRegionByEncodedName(hriPrimary.getEncodedName());
      break;
    case TERTIARY:
      region = getTertiaryRS().getRegionByEncodedName(hriPrimary.getEncodedName());
      break;
    }
    return region;
  }

  private HRegionServer getRegionServer(OpenedIn openedIn)
      throws NotServingRegionException {
    HRegionServer regionServer = null;
    switch (openedIn) {
    case PRIMARY:
      // If secondary is opened in Primary
      regionServer = getRS();
      break;
    case SECONDARY:
      regionServer = getSecondaryRS();
      break;
    case TERTIARY:
      regionServer = getTertiaryRS();
      break;
    }
    return regionServer;
  }

  private Region getTertiaryRegion(OpenedIn openedIn) throws NotServingRegionException {
    Region region = null;
    switch (openedIn) {
    case PRIMARY:
      // If secondary is opened in Primary
      region = getRS().getRegionByEncodedName(hriTertiary.getEncodedName());
      break;
    case SECONDARY:
      region = getSecondaryRS().getRegionByEncodedName(hriTertiary.getEncodedName());
      break;
    case TERTIARY:
      region = getTertiaryRS().getRegionByEncodedName(hriTertiary.getEncodedName());
      break;
    }
    return region;
  }

  private Region getSecondaryRegion(Pair<OpenedIn, OpenedIn> pair)
      throws NotServingRegionException {
    Region region = null;
    switch (pair.getSecond()) {
    case PRIMARY:
      // If secondary is opened in Primary
      region = getRS().getRegionByEncodedName(hriSecondary.getEncodedName());
      break;
    case SECONDARY:
      region = getSecondaryRS().getRegionByEncodedName(hriSecondary.getEncodedName());
      break;
    case TERTIARY:
      region = getTertiaryRS().getRegionByEncodedName(hriSecondary.getEncodedName());
      break;
    }
    return region;
  }

  private Pair<OpenedIn, OpenedIn> openSecondary() throws Exception {
    OpenedIn primaryOpenedIn = OpenedIn.PRIMARY;
    OpenedIn secondaryOpenedIn = OpenedIn.SECONDARY;
    Pair<OpenedIn, OpenedIn> pair = new Pair<OpenedIn, OpenedIn>();
    try {
      getRS().getRegion(hriPrimary.getRegionName());
    } catch (NotServingRegionException e) {
      try {
        getSecondaryRS().getRegion(hriPrimary.getRegionName());
        // primary is found in 2nd region server so open secondary in either 1 or 2
        primaryOpenedIn = OpenedIn.SECONDARY;
      } catch (NotServingRegionException e1) {
        // it is found in 3rd regionserver
        try {
          getTertiaryRS().getRegion(hriPrimary.getRegionName());
          primaryOpenedIn = OpenedIn.TERTIARY;
        } catch (NotServingRegionException e2) {
          throw new Exception("Region no where to be found ");
        }
      }
    }
    switch (primaryOpenedIn) {
    case PRIMARY:
      // open in secondary RS
      openRegion(HTU, getSecondaryRS(), hriSecondary);
      secondaryOpenedIn = OpenedIn.SECONDARY;
      break;
    case SECONDARY:
      // so open it in PRIMARY
      openRegion(HTU, getTertiaryRS(), hriSecondary);
      secondaryOpenedIn = OpenedIn.TERTIARY;
      break;
    case TERTIARY:
      openRegion(HTU, getRS(), hriSecondary);
      secondaryOpenedIn = OpenedIn.PRIMARY;
      break;
    }
    pair.setFirst(primaryOpenedIn);
    pair.setSecond(secondaryOpenedIn);
    return pair;
  }

  enum OpenedIn {
    PRIMARY, SECONDARY, TERTIARY;
  }
}