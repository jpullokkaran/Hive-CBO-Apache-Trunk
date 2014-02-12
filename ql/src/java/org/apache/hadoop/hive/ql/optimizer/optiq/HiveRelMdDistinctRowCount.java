package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMdDistinctRowCount;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util14.NumberUtil;

public class HiveRelMdDistinctRowCount extends RelMdDistinctRowCount {
  // Catch-all rule when none of the others apply.
  public Double getDistinctRowCount(RelNode rel, BitSet groupKey, RexNode predicate) {
    if (rel instanceof HiveTableScanRel) {
      return getDistinctRowCount((HiveTableScanRel) rel, groupKey, predicate);
    }

    return NumberUtil.multiply(RelMetadataQuery.getRowCount(rel),
        RelMetadataQuery.getSelectivity(rel, predicate));
  }

  private Double getDistinctRowCount(HiveTableScanRel htRel, BitSet groupKey, RexNode predicate) {
    List<Integer> projIndxLst = OptiqUtil.translateBitSetToProjIndx(groupKey);
    List<HiveColStat> colStats = htRel.getColStat(projIndxLst);
    Double noDistinctRows = 1.0;
    for (HiveColStat cStat : colStats) {
      noDistinctRows *= cStat.getNDV();
    }

    return Math.min(noDistinctRows, htRel.getRows());
  }

  public static Double getDistinctRowCount(RelNode r, int indx) {
    BitSet chilBitSet = new BitSet();
    chilBitSet.set(indx);
    return getDistinctRowCount(r, chilBitSet);
  }

  public static Double getDistinctRowCount(RelNode r, BitSet chilBitSet) {
    return RelMetadataQuery.getDistinctRowCount(r, chilBitSet, (RexNode) null);
  }
}
