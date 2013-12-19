package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

public class HiveIRShuffleRel extends HiveIRRel {
  private final List<Integer> m_partCols = new LinkedList<Integer>();

  public static HiveIRShuffleRel constructHiveIRShuffleRel(RelOptCluster cluster, RelTraitSet traitSet, List<Integer> partCols, HiveRel child) {
      List<RexNode> exps = OptiqUtil.constructChildInputRefs(child);
      RelDataType rowType = RexUtil.createStructType(
              cluster.getTypeFactory(), exps, child.getRowType().getFieldNames());
      return new HiveIRShuffleRel(cluster, traitSet, partCols, child, exps, rowType);
  }
  
  //TODO: Handle expressions as partition cols (x+Y)
  public HiveIRShuffleRel(RelOptCluster cluster, RelTraitSet traitSet, List<Integer> partCols, HiveRel child, List<RexNode> exps, RelDataType rowType) {
        super(cluster, OptiqTraitsUtil.getAggregateTraitSet(cluster, traitSet,
                partCols, null, child), child, exps, rowType, 0);
    m_partCols.addAll(partCols);
  }


  public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
      super.explainTerms(pw);
      for (Integer pColIndx : m_partCols) {
          pw.item("partitionCol" + pColIndx, exps.get(pColIndx));
      }
      return pw;
  }

  @Override
  public Double getEstimatedMemUsageInVertex() {
    //TODO: Even in shuffle case, Hive would store all data for a given key value from other relations will be kept in memory (except for the streaming side)
    // So actual mem consumption depends on a) If this is the streaming side b) what are the average size of partition from other relations
    return getAvgTupleSize();
  }

  public List<Integer> getPartCols() {
    return m_partCols;
  }
}
