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
package org.apache.hadoop.hbase;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This Cell is an implementation of {@link ByteBufferExtendedCell} where the data resides in
 * off heap/ on heap ByteBuffer
 */
@InterfaceAudience.Private
public class ByteBufferKeyValue extends ByteBufferKeyValueWithoutSeqId {

  private long seqId = 0;

  public static final int FIXED_OVERHEAD = ByteBufferKeyValueWithoutSeqId.FIXED_OVERHEAD
      + Bytes.SIZEOF_LONG;

  public ByteBufferKeyValue(ByteBuffer buf, int offset, int length, long seqId) {
    super(buf, offset, length);
    this.seqId = seqId;
  }

  public ByteBufferKeyValue(ByteBuffer buf, int offset, int length) {
    super(buf, offset, length);
  }

  @Override
  public long getSequenceId() {
    return this.seqId;
  }

  @Override
  public void setSequenceId(long seqId) {
    this.seqId = seqId;
  }

  @Override
  public long heapSize() {
    if (this.buf.hasArray()) {
      return ClassSize.align(FIXED_OVERHEAD + length);
    }
    return ClassSize.align(FIXED_OVERHEAD) + KeyValueUtil.length(this);
  }
}
