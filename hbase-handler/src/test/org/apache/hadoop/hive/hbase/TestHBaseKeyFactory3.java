package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.mapred.JobConf;

/**
 * Simple extension of {@link TestHBaseKeyFactory2} with exception of using filters instead of start
 * and stop keys
 * */
public class TestHBaseKeyFactory3 extends TestHBaseKeyFactory2 {

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc predicate) {
    TestHBasePredicateDecomposer decomposedPredicate = new TestHBasePredicateDecomposer(keyMapping);
    return decomposedPredicate.decomposePredicate(keyMapping.columnName, predicate);
  }
}

class TestHBasePredicateDecomposer extends AbstractHBaseKeyPredicateDecomposer {

  private static final int FIXED_LENGTH = 10;

  private ColumnMapping keyMapping;

  TestHBasePredicateDecomposer(ColumnMapping keyMapping) {
    this.keyMapping = keyMapping;
  }

  @Override
  public HBaseScanRange getScanRange(List<IndexSearchCondition> searchConditions)
      throws Exception {
    Map<String, List<IndexSearchCondition>> fieldConds =
        new HashMap<String, List<IndexSearchCondition>>();
    for (IndexSearchCondition condition : searchConditions) {
      String fieldName = condition.getFields()[0];
      List<IndexSearchCondition> fieldCond = fieldConds.get(fieldName);
      if (fieldCond == null) {
        fieldConds.put(fieldName, fieldCond = new ArrayList<IndexSearchCondition>());
      }
      fieldCond.add(condition);
    }
    Filter filter = null;
    HBaseScanRange range = new HBaseScanRange();

    StructTypeInfo type = (StructTypeInfo) keyMapping.columnType;
    for (String name : type.getAllStructFieldNames()) {
      List<IndexSearchCondition> fieldCond = fieldConds.get(name);
      if (fieldCond == null || fieldCond.size() > 2) {
        continue;
      }
      for (IndexSearchCondition condition : fieldCond) {
        if (condition.getConstantDesc().getValue() == null) {
          continue;
        }
        String comparisonOp = condition.getComparisonOp();
        String constantVal = String.valueOf(condition.getConstantDesc().getValue());

        byte[] valueAsBytes = toBinary(constantVal, FIXED_LENGTH, false, false);

        if (comparisonOp.endsWith("UDFOPEqual")) {
          filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(valueAsBytes));
        } else if (comparisonOp.endsWith("UDFOPEqualOrGreaterThan")) {
          filter = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(valueAsBytes));
        } else if (comparisonOp.endsWith("UDFOPGreaterThan")) {
          filter = new RowFilter(CompareOp.GREATER, new BinaryComparator(valueAsBytes));
        } else if (comparisonOp.endsWith("UDFOPEqualOrLessThan")) {
          filter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(valueAsBytes));
        } else if (comparisonOp.endsWith("UDFOPLessThan")) {
          filter = new RowFilter(CompareOp.LESS, new BinaryComparator(valueAsBytes));
        } else {
          throw new IOException(comparisonOp + " is not a supported comparison operator");
        }
      }
    }
    if (filter != null) {
      range.addFilter(filter);
    }
    return range;
  }

  private byte[] toBinary(String value, int max, boolean end, boolean nextBA) {
    return toBinary(value.getBytes(), max, end, nextBA);
  }

  private byte[] toBinary(byte[] value, int max, boolean end, boolean nextBA) {
    byte[] bytes = new byte[max + 1];
    System.arraycopy(value, 0, bytes, 0, Math.min(value.length, max));
    if (end) {
      Arrays.fill(bytes, value.length, max, (byte) 0xff);
    }
    if (nextBA) {
      bytes[max] = 0x01;
    }
    return bytes;
  }
}