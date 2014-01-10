package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

public abstract class HiveIRRel extends ProjectRelBase implements HiveRel {

  public HiveIRRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, List<RexNode> exps, RelDataType rowType, int flags) {
    super(cluster, traitSet, child, exps, rowType, flags);
  }

  @Override
  public double getAvgTupleSize() {
      return (OptiqUtil.getNonSubsetRelNode(getChild())).getAvgTupleSize();
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
  public void implement(Implementor implementor) {
    // TODO Auto-generated method stub
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
