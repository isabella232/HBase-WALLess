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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRegionReplicaReplicator {

  private MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

  @Test
  public void testPullEntries() throws Exception {
    Set<byte[]> families = new HashSet<>();
    families.add(Bytes.toBytes("f1"));
    RegionInfo regionInfo = new HRegionInfo(1234, TableName.META_TABLE_NAME, 0);
    RegionReplicaReplicator replicator = new RegionReplicaReplicator(null, regionInfo, mvcc,
        families, 1, 0, 3);
    MemstoreReplicationEntry e1 = createEntry(false);
    MemstoreReplicationEntry e2 = createEntry(false);
    MemstoreReplicationEntry e3 = createEntry(false);
    replicator.append(e1);
    replicator.append(e2);
    replicator.append(e3);
    List<MemstoreReplicationEntry> entries = replicator.pullEntries(1);
    assertEquals(3, entries.size());
    assertEquals(e1, entries.get(0));
    assertEquals(e2, entries.get(1));
    assertEquals(e3, entries.get(2));
    entries = replicator.pullEntries(2);
    assertNull(entries);
    entries = replicator.pullEntries(3);
    assertNull(entries);
    MemstoreReplicationEntry e4 = createEntry(false);
    MemstoreReplicationEntry e5 = createEntry(false);
    replicator.append(e4);
    replicator.append(e5);
    MemstoreReplicationEntry e6 = createEntry(true);
    replicator.append(e6);
    MemstoreReplicationEntry e7 = createEntry(false);
    replicator.append(e7);
    entries = replicator.pullEntries(4);
    assertEquals(2, entries.size());
    assertEquals(e4, entries.get(0));
    assertEquals(e5, entries.get(1));
    entries = replicator.pullEntries(5);
    assertNull(entries);
    entries = replicator.pullEntries(6);
    assertEquals(1, entries.size());
    assertEquals(e6, entries.get(0));
    entries = replicator.pullEntries(7);
    assertEquals(1, entries.size());
    assertEquals(e7, entries.get(0));
  }

  private MemstoreReplicationEntry createEntry(boolean metaMarkerReq) {
    MemstoreReplicationKey key = new MemstoreReplicationKey(Bytes.toBytes("1234"), 0);
    MemstoreEdits edits = new MemstoreEdits();
    edits.add(new KeyValue());
    MemstoreReplicationEntry entry = new MemstoreReplicationEntry(key, edits, metaMarkerReq);
    return entry;
  }
}
