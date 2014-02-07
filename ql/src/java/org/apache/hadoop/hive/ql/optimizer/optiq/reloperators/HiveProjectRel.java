package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.OptiqStatsUtil;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

public class HiveProjectRel extends ProjectRelBase implements HiveRel {
    private final List<Integer> m_virtualCols;
    
    private List<Integer> m_bucketingColsTraitToPropagate;
    private List<Integer> m_bucketingSortColsTraitToPropagate;
    private List<Integer> m_sortColsTraitToPropagate;

    /**
     * Creates a HiveProjectRel.
     * 
     * @param cluster
     *            Cluster this relational expression belongs to
     * @param child
     *            input relational expression
     * @param exps
     *            List of expressions for the input columns
     * @param rowType
     *            output row type
     * @param flags
     *            values as in {@link ProjectRelBase.Flags}
     */
    public HiveProjectRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
        List<RexNode> exps, RelDataType rowType, int flags) {
      super(cluster, traitSet, child, exps, rowType, flags);
      m_virtualCols = ImmutableList.copyOf(OptiqUtil.getVirtualCols(exps));
    }

  /**
   * Creates a HiveProjectRel with no sort keys.
   *
   * @param child
   *            input relational expression
   * @param exps
   *            set of expressions for the input columns
   * @param fieldNames
 *            aliases of the expressions
   */
  public static HiveProjectRel create(RelNode child, List<RexNode> exps, List<String> fieldNames) {
    RelOptCluster cluster = child.getCluster();
    RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), exps, fieldNames);
    return create(cluster, child, exps, rowType, Collections.<RelCollation>emptyList());
  }

  /**
   * Creates a HiveProjectRel.
   */
  public static HiveProjectRel create(RelOptCluster cluster, RelNode child,
      List<RexNode> exps, RelDataType rowType, final List<RelCollation> collationList) {
    RelTraitSet traitSet = OptiqTraitsUtil.getSelectTraitSet(cluster, exps, child);
    return new HiveProjectRel(cluster, traitSet, child, exps, rowType, Flags.BOXED);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(HiveRel.CONVENTION);
    return new HiveProjectRel(getCluster(), traitSet, sole(inputs),
        getProjects(), rowType, getFlags());
  }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      return HiveCost.FACTORY.makeZeroCost();
    }

    public void implement(Implementor implementor) {
    }

    public List<Integer> getVirtualCols() {
        return m_virtualCols;
    }
    
    @Override
    public double getAvgTupleSize() {
        return OptiqStatsUtil.computeProjectRelAvgTupleSize(this);
    }

    @Override
    public List<HiveColStat> getColStat(List<Integer> projIndxLst) {
        return OptiqStatsUtil.computeProjectRelColStat(this, projIndxLst);
    }

    @Override
    public HiveColStat getColStat(Integer projIndx) {
        return OptiqStatsUtil.computeColStat(this, projIndx);
    }

    @Override
    public Double getEstimatedMemUsageInVertex() {
        return ((HiveRel) getChild()).getEstimatedMemUsageInVertex()
                + OptiqStatsUtil.getAvgSize(exps,
                        OptiqUtil.getNonSubsetRelNode(getChild()), true);
    }

    @Override
    public boolean propagateBucketingTraitUpwardsViaTransformation(
            List<Integer> bucketingCols, List<Integer> bucketSortCols) {

        boolean willPropagteBucketing = false;
        if (m_bucketingColsTraitToPropagate != null) {
            if (m_bucketingColsTraitToPropagate.equals(bucketingCols)
                    && (((m_bucketingSortColsTraitToPropagate == null) && (bucketSortCols == null)) || ((m_bucketingSortColsTraitToPropagate != null)
                            && (bucketSortCols != null) && OptiqUtil
                                .orderedSubset(
                                        m_bucketingSortColsTraitToPropagate,
                                        bucketSortCols)))) {
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
    public boolean propagateSortingTraitUpwardsViaTransformation(
            List<Integer> sortingCols) {
        boolean willPropagteSorting = false;

        if (m_sortColsTraitToPropagate != null) {
            if (sortingCols == null) {
                m_sortColsTraitToPropagate = null;
            } else {
                if (OptiqUtil.orderedSubset(m_sortColsTraitToPropagate,
                        sortingCols)) {
                    willPropagteSorting = true;
                }
            }
        }

        return willPropagteSorting;
    }

    @Override
    public boolean shouldPropagateTraitFromChildViaTransformation(
            RelBucketing bucketTraitFromChild) {
        if (m_bucketingColsTraitToPropagate != null) {
            if (bucketTraitFromChild.getPartitionCols().contains(
                    m_bucketingColsTraitToPropagate)) {
                if ((bucketTraitFromChild.getSortingCols() != null
                        && m_bucketingSortColsTraitToPropagate != null && OptiqUtil
                            .orderedSubset(
                                    bucketTraitFromChild.getSortingCols(),
                                    m_bucketingSortColsTraitToPropagate))
                        || (bucketTraitFromChild.getSortingCols() == null && m_bucketingSortColsTraitToPropagate == null)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean shouldPropagateTraitFromChildViaTransformation(
            RelCollation sortTraitFromChild) {
        if (m_sortColsTraitToPropagate != null) {
            List<Integer> sortCols = OptiqTraitsUtil
                    .getSortingColPos(sortTraitFromChild);
            if (OptiqUtil.orderedSubset(sortCols, m_sortColsTraitToPropagate)) {
                return true;
            }
        }
        return false;
    }
}
