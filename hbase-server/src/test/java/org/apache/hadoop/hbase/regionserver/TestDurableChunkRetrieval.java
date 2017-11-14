package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestDurableChunkRetrieval {
  private static final Log LOG = LogFactory.getLog(TestDurableChunkRetrieval.class);

  private static final int NB_SERVERS = 1;
  private static Table table;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;
  static {
    File file = new File("./chunkfile");
    if(file.exists()) {
      file.delete();
    }
  }
  @BeforeClass
  public static void before() throws Exception {
    HTU.getConfiguration().setInt("hbase.master.wait.on.regionservers.mintostart", 1);
    HTU.getConfiguration().setBoolean("hbase.hregion.memstore.mslab.enabled", true);
    HTU.getConfiguration().setInt("hbase.hregion.memstore.chunkpool.maxsize", 1);
    HTU.getConfiguration().setInt("hbase.hregion.memstore.chunkpool.initialsize", 1);
    HTU.getConfiguration().setInt("hbase.regionserver.offheap.global.memstore.size", 800);
    HTU.getConfiguration().setBoolean("hbase.regionserver.use.mslab.systemtables.for.tests", false);
    HTU.getConfiguration().setBoolean("hbase.balancer.tablesOnMaster.systemTablesOnly", false);
    HTU.getConfiguration().setBoolean("hbase.balancer.tablesOnMaster", false);
    HTU.getConfiguration().set("hbase.memstore.mslab.durable.path", "./chunkfile");
    HTU.startMiniCluster(NB_SERVERS);
    Thread.sleep(3000);
    final TableName tableName =
        TableName.valueOf(TestDurableChunkRetrieval.class.getSimpleName());

    // Create table then get the single region for our new table.
    createTableDirectlyFromHTD(tableName);
  }

  private static void createTableDirectlyFromHTD(final TableName tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    //htd.setRegionReplication(1);
    // create a table with 3 replication

    table = HTU.createTable(htd, new byte[][] { f }, null,
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

  @Test
  public void testDurableChunkRetrieval() throws Exception {
    byte[] val = new byte[10240];
    List<Put> puts = new ArrayList<Put>();
    for (int i = 0; i < 3000; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(f, Bytes.toBytes("q"), val);
      puts.add(p);
    }
    table.put(puts);
    System.out.println("completed puts ");
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    Iterator<Result> iterator = scanner.iterator();
    int count= 0 ;
    while(iterator.hasNext()) {
      Result next = iterator.next();
      count++;
    }
    System.out.println("Before abort results count is "+count);
    // doing this would mean that while retrieval we will not have any chunks to read data
    //HTU.getAdmin().flush(table.getName());
    for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      for (Region r : rs.getRegionServer().getOnlineRegions(table.getName())) {
        // now see how does the assignment happen
        rs.getRegionServer().abort("for test");
        break;
      }
    }
    // start new region server.
    // If i move the sleep to after the start of new RS then it is creating lot of issues.  Need to debug
    Thread.sleep(5000);
    HTU.getHBaseCluster().startRegionServer();
    Thread.sleep(5000);
    s = new Scan();
    scanner = table.getScanner(s);
    iterator = scanner.iterator();
    count= 0 ;
    while(iterator.hasNext()) {
      Result next = iterator.next();
      count++;
    }
    assertEquals("The total rows received should be 3000", count, 3000);
    // flush this data
    HTU.getAdmin().flush(table.getName());
    
    // now load some more data
    puts = new ArrayList<Put>();
    for (int i = 0; i < 300; i++) {
      Put p = new Put(Bytes.toBytes("abcd" + i));
      p.addColumn(f, Bytes.toBytes("q"), val);
      puts.add(p);
    }
    table.put(puts);
    // now again kill and check. This time also we should be able to read but ensure we read only the latest
    // data from the chunks and not the old data.
    for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      for (Region r : rs.getRegionServer().getOnlineRegions(table.getName())) {
        // now see how does the assignment happen
        rs.getRegionServer().abort("for test");
        break;
      }
    }
    // start new region server.
    // If i move the sleep to after the start of new RS then it is creating lot of issues.  Need to debug
    Thread.sleep(5000);
    HTU.getHBaseCluster().startRegionServer();
    Thread.sleep(5000);
    s = new Scan();
    s.setStartRow(Bytes.toBytes("abcd" + 0));
    s.setStopRow(Bytes.toBytes("abcde"));
    scanner = table.getScanner(s);
    iterator = scanner.iterator();
    count= 0 ;
    while(iterator.hasNext()) {
      Result next = iterator.next();
      count++;
    }
    assertEquals("The total rows received should be 300", count, 300);
  }
}
