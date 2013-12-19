package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;

public interface HiveRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in MongoDB. */
  final Convention CONVENTION = new Convention.Impl("HIVE", HiveRel.class);

  class Implementor {

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((HiveRel) input).implement(this);
    }
  }

    /**
     * Get column stats for given projection/column indexes.
     * 
     * @param projIndxLst
     *            List of projection indexes. If null then all of the colStat
     *            for this node is returned.
     * @return List of Colstat in the same order as the projIndxLst
     */
  public List<HiveColStat> getColStat(List<Integer> projIndxLst);
  
  /**
   * Get Column stat for given projection index.
   * 
   * @param projIndx index of projection with in the schema
   * @return column stat
   */
  public HiveColStat getColStat(Integer projIndx);

  /**
   * Get average tuple size.
   * @return long
   */
  public double getAvgTupleSize();

  /**
   * Get max memory used so far with in the physical process boundary.
   * This sums up the memory consumption of each operator in the process boundary.
   *
   * @return Double Estimated memory usage of this node.
   */
  public Double getEstimatedMemUsageInVertex();

  /**
   * Propagate requested bucketing trait upwards if possible
   *
   * @return true if this node will propagate requested bucketing  trait.
   */
  public boolean propagateBucketingTraitUpwardsViaTransformation(List<Integer> bucketingCols, List<Integer> bucketSortCols);

  /**
   * Propagate requested sorting Trait upwards if possible
   */
  public boolean propagateSortingTraitUpwardsViaTransformation(List<Integer> sortingCols);

  /**
   * Should propagate bucketing trait from child Node?
   * @param bucketTraitFromChild TODO
   */
  public boolean shouldPropagateTraitFromChildViaTransformation(RelBucketing bucketTraitFromChild);

  /**
   * Should propagate sorting trait from child Node?
   */
  public boolean shouldPropagateTraitFromChildViaTransformation(RelCollation sortTraitFromChild);
}
