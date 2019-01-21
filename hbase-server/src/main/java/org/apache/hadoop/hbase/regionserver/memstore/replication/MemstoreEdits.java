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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is similar to WALEdit, but mainly encapsulates all the cells corresponding to a transaction
 * that goes into the memstore. TODO : USe this for now
 */
@InterfaceAudience.Private
public class MemstoreEdits {
  private Map<byte[], List<Cell>> familyMap = 
      new TreeMap<byte[], List<Cell>>(Bytes.BYTES_COMPARATOR);

  public MemstoreEdits() {
    familyMap = new TreeMap<byte[], List<Cell>>(Bytes.BYTES_COMPARATOR);
  }

  public MemstoreEdits add(byte[] fam, Cell cell) {
    List<Cell> cellList = familyMap.get(fam);
    if (cellList == null) {
      cellList = new ArrayList<Cell>();
      familyMap.put(fam, cellList);
    }
    cellList.add(cell);
    return this;
  }

  public MemstoreEdits add(byte[] fam, List<Cell> cells) {
    List<Cell> cellList = familyMap.get(fam);
    if (cellList == null) {
      cellList = new ArrayList<Cell>();
      familyMap.put(fam, cellList);
    }
    cellList.addAll(cells);
    return this;
  }

  public boolean isEmpty() {
    return familyMap.isEmpty();
  }

  public int size() {
    int cellsCount = 0;
    for (List<Cell> cells : familyMap.values()) {
      cellsCount += cells.size();
    }
    return cellsCount;
  }

  public Map<byte[], List<Cell>> getFamMap() {
    return this.familyMap;
  }

  public long heapSize() {
    // Revisit
    long ret = ClassSize.TREEMAP;
    for (List<Cell> cells : familyMap.values()) {
      for (Cell cell : cells) {
        ret += CellUtil.estimatedHeapSizeOf(cell);
      }
    }
    return ret;
  }

  public int getSize() {
    return familyMap.values().size();
  }
}
