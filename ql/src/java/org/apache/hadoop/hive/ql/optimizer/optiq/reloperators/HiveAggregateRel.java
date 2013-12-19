package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.OptiqStatsUtil;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

public class HiveAggregateRel extends AggregateRelBase implements HiveRel {
  public HiveAggregateRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      BitSet groupSet,
      List<AggregateCall> aggCalls)
      throws InvalidRelException {
    super(cluster, OptiqTraitsUtil.getAggregateTraitSet(cluster, traitSet,
        OptiqUtil.translateBitSetToProjIndx(groupSet), aggCalls, child), child, groupSet, aggCalls);
    assert getConvention() instanceof HiveRel;

    for (AggregateCall aggCall : aggCalls) {
      if (aggCall.isDistinct()) {
        throw new InvalidRelException(
            "distinct aggregation not supported");
      }
    }
  }

  @Override
  public HiveAggregateRel copy(
      RelTraitSet traitSet, List<RelNode> inputs) {
    try {
      return new HiveAggregateRel(
          getCluster(),
          traitSet,
          sole(inputs),
          groupSet,
          aggCalls);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  public void implement(Implementor implementor) {
  }

  @Override
  public double getAvgTupleSize() {
      return (OptiqUtil.getNonSubsetRelNode(getChild())).getAvgTupleSize();
  }

  @Override
  public Double getEstimatedMemUsageInVertex() {
    return (((HiveRel) getChild()).getEstimatedMemUsageInVertex() + getAvgTupleSize());
  }

  @Override
  public List<HiveColStat> getColStat(List<Integer> projIndxLst) {
      return (OptiqUtil.getNonSubsetRelNode(getChild())).getColStat(projIndxLst);
  }

  @Override
  public HiveColStat getColStat(Integer projIndx) {
      return (OptiqUtil.getNonSubsetRelNode(getChild())).getColStat(projIndx);
  }

  @Override
  public boolean propagateBucketingTraitUpwardsViaTransformation(List<Integer> bucketingCols, List<Integer> bucketSortCols){
    return false;
  }

  @Override
  public boolean propagateSortingTraitUpwardsViaTransformation(List<Integer> sortingCols) {
    return false;
  }

  @Override
  public boolean shouldPropagateTraitFromChildViaTransformation(RelBucketing bucketTraitFromChild) {
    return false;
  }

  @Override
  public boolean shouldPropagateTraitFromChildViaTransformation(RelCollation sortTraitFromChild) {
    return false;
  }
}
