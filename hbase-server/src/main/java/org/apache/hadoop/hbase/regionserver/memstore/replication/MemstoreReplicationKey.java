/**
 *
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class represents the region info, table name for the memstore edits that represents a
 * transaction - TODO : Merge this and MemstoreEdits to one class?
 */
@InterfaceAudience.Private
public class MemstoreReplicationKey {
  /**
   */
  public static final long NO_SEQUENCE_ID = -1;
  private byte [] encodedRegionName;

  private TableName tablename;
  /**
   * SequenceId for this edit. Set post-construction at write-to-WAL time. Until then it is
   * NO_SEQUENCE_ID. Change it so multiple threads can read it -- e.g. access is synchronized.
   */
  private long sequenceId;

  /**
   * Used during WAL replay; the sequenceId of the edit when it came into the system.
   */
  private long origLogSeqNum = 0;

  /** Time at which this edit was written. */
  private long writeTime;

  private MultiVersionConcurrencyControl mvcc;
  
  /**
   * Set in a way visible to multiple threads; e.g. synchronized getter/setters.
   */
  private MultiVersionConcurrencyControl.WriteEntry writeEntry;
  public static final List<UUID> EMPTY_UUIDS = Collections.unmodifiableList(new ArrayList<UUID>());

  // TODO : Support compression, nonceGroup and nonces, inter cluster replication
  public MemstoreReplicationKey(byte[] encodedRegionName, TableName tableName,
      MultiVersionConcurrencyControl mvcc) {
    this(encodedRegionName, tableName, NO_SEQUENCE_ID, HConstants.LATEST_TIMESTAMP, mvcc);
  }

  // TODO : Support compression, nonceGroup and nonces, inter cluster replication
  public MemstoreReplicationKey(byte[] encodedRegionName, TableName tableName, long now,
      MultiVersionConcurrencyControl mvcc) {
    this(encodedRegionName, tableName, NO_SEQUENCE_ID, now, mvcc);
  }

  // TODO : Support compression, nonceGroup and nonces, inter cluster replication
  public MemstoreReplicationKey(byte[] encodedRegionName, TableName tableName, long sequenceId,
      long now, MultiVersionConcurrencyControl mvcc) {
    this.encodedRegionName = encodedRegionName;
    this.tablename = tableName;
    this.sequenceId = sequenceId;
    this.writeTime = now;
    this.mvcc = mvcc;
  }

  public byte[] getEncodedRegionName() {
    return this.encodedRegionName;
  }

  public TableName getTableName() {
    return this.tablename;
  }

  public long getWriteTime() {
    return this.writeTime;
  }

  public MultiVersionConcurrencyControl getMvcc() {
    return this.mvcc;
  }
  
  /**
   * Drop this instance's tablename byte array and instead
   * hold a reference to the provided tablename. This is not
   * meant to be a general purpose setter - it's only used
   * to collapse references to conserve memory.
   */
  void internTableName(TableName tablename) {
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert tablename.equals(this.tablename);
    this.tablename = tablename;
  }
  
  public void setSequenceId(long seqId) {
    this.sequenceId = seqId;
  }
  
  public long getSequenceId() {
    return this.sequenceId;
  }

  /**
   * Drop this instance's region name byte array and instead
   * hold a reference to the provided region name. This is not
   * meant to be a general purpose setter - it's only used
   * to collapse references to conserve memory.
   */
  void internEncodedRegionName(byte []encodedRegionName) {
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert Bytes.equals(this.encodedRegionName, encodedRegionName);
    this.encodedRegionName = encodedRegionName;
  }

  // Add proto file for this. PD serDe methods to be added
}
