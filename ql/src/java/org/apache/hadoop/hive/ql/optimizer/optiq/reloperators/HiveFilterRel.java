package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCostUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class HiveFilterRel extends FilterRelBase implements HiveRel {
  private List<Integer> m_bucketingColsTraitToPropagate;
  private List<Integer> m_bucketingSortColsTraitToPropagate;
  private List<Integer> m_sortColsTraitToPropagate;

  public HiveFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
      RexNode condition) {
    super(cluster, OptiqTraitsUtil.getFilterTraitSet(cluster, traits, child), child, condition);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(HiveRel.CONVENTION);
    return new HiveFilterRel(
        getCluster(), traitSet,
        sole(inputs),
        getCondition());
  }

  @Override
  public void implement(Implementor implementor) {
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
  	return HiveCostUtil.computeCost(this);
  }

  @Override
  public double getAvgTupleSize() {
      return (OptiqUtil.getNonSubsetRelNode(getChild())).getAvgTupleSize();
  }

  @Override
  public Double getEstimatedMemUsageInVertex() {
      return OptiqUtil.getNonSubsetRelNode(getChild()).getEstimatedMemUsageInVertex();
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
  public boolean propagateBucketingTraitUpwardsViaTransformation(List<Integer> bucketingCols,
      List<Integer> bucketSortCols) {

    boolean willPropagteBucketing = false;
    if (m_bucketingColsTraitToPropagate != null) {
      if (m_bucketingColsTraitToPropagate.equals(bucketingCols)
          && (((m_bucketingSortColsTraitToPropagate == null) && (bucketSortCols == null)) || ((m_bucketingSortColsTraitToPropagate != null)
              && (bucketSortCols != null) && OptiqUtil.orderedSubset(m_bucketingSortColsTraitToPropagate, bucketSortCols)))) {
        willPropagteBucketing = true;
      }
    } else {
      m_bucketingColsTraitToPropagate = bucketingCols;
      m_bucketingColsTraitToPropagate = bucketSortCols;
      willPropagteBucketing = true;
    }

    return willPropagteBucketing;
  }

  @Override
  public boolean propagateSortingTraitUpwardsViaTransformation(List<Integer> sortingCols) {
    boolean willPropagteSorting = false;

    if (m_sortColsTraitToPropagate != null) {
      if (sortingCols == null) {
        m_sortColsTraitToPropagate = null;
      }else {
      if (OptiqUtil.orderedSubset(m_sortColsTraitToPropagate, sortingCols)) {
        willPropagteSorting = true;
      }
      }
    }

    return willPropagteSorting;
  }

  @Override
  public boolean shouldPropagateTraitFromChildViaTransformation(RelBucketing bucketTraitFromChild) {
    if (m_bucketingColsTraitToPropagate != null) {
      if (bucketTraitFromChild.getPartitionCols().contains(m_bucketingColsTraitToPropagate)) {
        if ((bucketTraitFromChild.getSortingCols() != null
            && m_bucketingSortColsTraitToPropagate != null
            && OptiqUtil.orderedSubset(bucketTraitFromChild.getSortingCols(), m_bucketingSortColsTraitToPropagate))
            || (bucketTraitFromChild.getSortingCols() == null && m_bucketingSortColsTraitToPropagate == null)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean shouldPropagateTraitFromChildViaTransformation(RelCollation sortTraitFromChild) {
    if (m_sortColsTraitToPropagate != null) {
      List<Integer> sortCols = OptiqTraitsUtil.getSortingColPos(sortTraitFromChild);
      if (OptiqUtil.orderedSubset(sortCols, m_sortColsTraitToPropagate)) {
          return true;
      }
    }
    return false;
  }
}
