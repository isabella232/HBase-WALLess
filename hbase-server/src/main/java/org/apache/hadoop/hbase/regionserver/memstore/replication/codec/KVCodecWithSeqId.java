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
package org.apache.hadoop.hbase.regionserver.memstore.replication.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.nio.ByteBuff;

/**
 * The format of writing Cells is <4 bytes KV length><Key/Value/Tags><8 bytes seqId>
 * Note that the first 4 bytes of length will NOT include the seqId serialized size.
 */
@InterfaceAudience.Private
public class KVCodecWithSeqId implements Codec {

  public static class KeyValueEncoder extends KeyValueCodecWithTags.KeyValueEncoder {
    public KeyValueEncoder(OutputStream out) {
      super(out);
    }

    @Override
    public void write(Cell cell) throws IOException {
      super.write(cell);
      // Write the seqId
      StreamUtils.writeLong(this.out, cell.getSequenceId());// TODO add BBUtils.putLong
    }
  }

  public static class KeyValueDecoder extends KeyValueCodecWithTags.KeyValueDecoder {
    public KeyValueDecoder(InputStream in) {
      super(in);
    }

    @Override
    protected Cell parseCell() throws IOException {
      Cell cell = super.parseCell();
      // Read the 8 bytes seqId
      long seqId = StreamUtils.readLong(this.in);
      CellUtil.setSequenceId(cell, seqId);
      return cell;
    }
  }

  public static class ByteBuffKeyValueDecoder
      extends KeyValueCodecWithTags.ByteBuffKeyValueDecoder {
    public ByteBuffKeyValueDecoder(ByteBuff buf) {
      super(buf);
    }

    @Override
    protected Cell createCell(byte[] buf, int offset, int len) throws IOException {
      Cell cell = super.createCell(buf, offset, len);
      CellUtil.setSequenceId(cell, readSeqId());
      return cell;
    }

    @Override
    protected Cell createCell(ByteBuffer bb, int pos, int len) throws IOException {
      Cell cell = super.createCell(bb, pos, len);
      CellUtil.setSequenceId(cell, readSeqId());
      return cell;
    }

    private long readSeqId() {
      return this.buf.getLong();
    }
  }

  @Override
  public Decoder getDecoder(InputStream is) {
    return new KeyValueDecoder(is);
  }

  @Override
  public Decoder getDecoder(ByteBuff buf) {
    return new ByteBuffKeyValueDecoder(buf);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return new KeyValueEncoder(os);
  }
}
