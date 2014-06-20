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
package org.apache.hadoop.hive.ql.io.orc;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndexEntry;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;

/**
 * A tool for printing out the file structure of ORC files.
 */
public final class FileDump {
  private static final String ROWINDEX_PREFIX = "--rowindex=";

  // not used
  private FileDump() {}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    List<String> files = new ArrayList<String>();
    List<Integer> rowIndexCols = null;
    for (String arg : args) {
      if (arg.startsWith("--")) {
        if (arg.startsWith(ROWINDEX_PREFIX)) {
          String[] colStrs = arg.substring(ROWINDEX_PREFIX.length()).split(",");
          rowIndexCols = new ArrayList<Integer>(colStrs.length);
          for (String colStr : colStrs) {
            rowIndexCols.add(Integer.parseInt(colStr));
          }
        } else {
          System.err.println("Unknown argument " + arg);
        }
      } else {
        files.add(arg);
      }
    }

    for (String filename : files) {
      System.out.println("Structure for " + filename);
      Path path = new Path(filename);
      Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
      RecordReaderImpl rows = (RecordReaderImpl) reader.rows();
      System.out.println("Rows: " + reader.getNumberOfRows());
      System.out.println("Compression: " + reader.getCompression());
      if (reader.getCompression() != CompressionKind.NONE) {
        System.out.println("Compression size: " + reader.getCompressionSize());
      }
      System.out.println("Type: " + reader.getObjectInspector().getTypeName());
      System.out.println("\nStripe Statistics:");
      Metadata metadata = reader.getMetadata();
      for (int n = 0; n < metadata.getStripeStatistics().size(); n++) {
        System.out.println("  Stripe " + (n + 1) + ":");
        StripeStatistics ss = metadata.getStripeStatistics().get(n);
        for (int i = 0; i < ss.getColumnStatistics().length; ++i) {
          System.out.println("    Column " + i + ": " +
              ss.getColumnStatistics()[i].toString());
        }
      }
      ColumnStatistics[] stats = reader.getStatistics();
      System.out.println("\nFile Statistics:");
      for(int i=0; i < stats.length; ++i) {
        System.out.println("  Column " + i + ": " + stats[i].toString());
      }
      System.out.println("\nStripes:");
      int stripeIx = -1;
      for (StripeInformation stripe : reader.getStripes()) {
        ++stripeIx;
        long stripeStart = stripe.getOffset();
        System.out.println("  Stripe: " + stripe.toString());
        OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);
        long sectionStart = stripeStart;
        for(OrcProto.Stream section: footer.getStreamsList()) {
          System.out.println("    Stream: column " + section.getColumn() +
            " section " + section.getKind() + " start: " + sectionStart +
            " length " + section.getLength());
          sectionStart += section.getLength();
        }
        for (int i = 0; i < footer.getColumnsCount(); ++i) {
          OrcProto.ColumnEncoding encoding = footer.getColumns(i);
          StringBuilder buf = new StringBuilder();
          buf.append("    Encoding column ");
          buf.append(i);
          buf.append(": ");
          buf.append(encoding.getKind());
          if (encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY) {
            buf.append("[");
            buf.append(encoding.getDictionarySize());
            buf.append("]");
          }
          System.out.println(buf);
        }
        if (rowIndexCols != null) {
          RowIndex[] indices = rows.readRowIndex(stripeIx);
          for (int col : rowIndexCols) {
            StringBuilder buf = new StringBuilder();
            buf.append("    Column ").append(col).append(": row index");
            RowIndex index = null;
            if ((col >= indices.length) || ((index = indices[col]) == null)) {
              buf.append(" not found\n");
              continue;
            }
            for (int entryIx = 0; entryIx < index.getEntryCount(); ++entryIx) {
              buf.append("      RG ").append(entryIx).append(": ");
              RowIndexEntry entry = index.getEntry(entryIx);
              if (entry == null) {
                buf.append("unknown\n");
                continue;
              }
              OrcProto.ColumnStatistics colStats = entry.getStatistics();
              if (colStats == null) {
                buf.append("no stats at ");
              } else {
                ColumnStatistics cs = ColumnStatisticsImpl.deserialize(colStats);
                Object min = RecordReaderImpl.getMin(cs), max = RecordReaderImpl.getMax(cs);
                buf.append("[").append(min).append(", ").append(max).append(") at ");
              }
              for (int posIx = 0; posIx < entry.getPositionsCount(); ++posIx) {
                if (posIx != 0) {
                  buf.append(",");
                }
                buf.append(entry.getPositions(posIx));
              }
              buf.append("\n");
            }
            System.out.println(buf);
          }
        }
      }
      rows.close();
    }
  }
}
