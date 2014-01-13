package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.OptiqStatsUtil;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

//TODO: Should we convert MultiJoin to be a child of HiveJoinRelBase
public class HiveJoinRel extends JoinRelBase implements HiveRel {
    // NOTE: COMMON_JOIN & SMB_JOIN are Sort Merge Join (in case of COMMON_JOIN
    // each parallel
    // computation
    // handles multiple splits where as in case of SMB each parallel computation
    // handles one bucket).
    // MAP_JOIN and BUCKET_JOIN is hash joins where MAP_JOIN keeps the whole
    // data set of non streaming
    // tables in memory where as BUCKET_JOIN keeps only the b
    public enum JoinAlgorithm {
        NONE, COMMON_JOIN, MAP_JOIN, BUCKET_JOIN, SMB_JOIN
    };

    public enum MapJoinStreamingRelation {
        NONE, LEFT_RELATION, RIGHT_RELATION
    };

    private JoinPredicateInfo m_jpi;
    private JoinAlgorithm m_joinAlgorithm = JoinAlgorithm.NONE;
    private MapJoinStreamingRelation m_mapJoinStreamingSide = MapJoinStreamingRelation.NONE;

    public static HiveJoinRel getJoin(RelOptCluster cluster, RelNode left,
            RelNode right, RexNode condition, JoinRelType joinType) {
        try {
        	Set<String> variablesStopped = Collections.emptySet();
            return new HiveJoinRel(cluster, null, left, right, condition,
                    joinType, variablesStopped, JoinAlgorithm.NONE, null);
        } catch (InvalidRelException e) {
            throw new RuntimeException(e);
        }
    }

    protected HiveJoinRel(RelOptCluster cluster, RelTraitSet traits,
            RelNode left, RelNode right, RexNode condition,
            JoinRelType joinType, Set<String> variablesStopped,
            JoinAlgorithm joinAlgo,
            MapJoinStreamingRelation streamingSideForMapJoin)
            throws InvalidRelException {
        super(cluster, OptiqTraitsUtil.getJoinTraitSet(cluster, traits), left,
                right, condition, joinType, variablesStopped);
        final List<Integer> leftKeys = new ArrayList<Integer>();
        final List<Integer> rightKeys = new ArrayList<Integer>();
        RexNode remaining = RelOptUtil.splitJoinCondition(left, right,
                condition, leftKeys, rightKeys);
        if (!remaining.isAlwaysTrue()) {
            throw new InvalidRelException(
                    "EnumerableJoinRel only supports equi-join");
        }
    }

    @Override
    public void implement(Implementor implementor) {
    }

    @Override
    public HiveJoinRel copy(RelTraitSet traitSet, RexNode conditionExpr,
            RelNode left, RelNode right) {
        try {
            return new HiveJoinRel(getCluster(), traitSet, left, right,
                    conditionExpr, joinType, variablesStopped, m_joinAlgorithm,
                    m_mapJoinStreamingSide);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    public HiveJoinRel copy(RelTraitSet traitSet, RexNode conditionExpr,
            RelNode left, RelNode right, JoinAlgorithm joinalgo,
            MapJoinStreamingRelation streamingSide) {
        try {
            return new HiveJoinRel(getCluster(), traitSet, left, right,
                    conditionExpr, joinType, variablesStopped, joinalgo,
                    streamingSide);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    public void setJoinAlgorithm(JoinAlgorithm algo) {
        m_joinAlgorithm = algo;
    }

    public JoinAlgorithm getJoinAlgorithm() {
        return m_joinAlgorithm;
    }

    public void setMapJoinStreamingSide(MapJoinStreamingRelation streamingSide) {
        m_mapJoinStreamingSide = streamingSide;
    }

    public MapJoinStreamingRelation getMapJoinStreamingSide() {
        return m_mapJoinStreamingSide;
    }

    public void setJoinPredicateInfo(JoinPredicateInfo jpi) {
        m_jpi = jpi;
    }

    public JoinPredicateInfo getJoinPredicateInfo() {
        return m_jpi;
    }

//    @Override
    public RelOptCost _computeSelfCost(RelOptPlanner planner) {
        // We always "build" the
        double rowCount = RelMetadataQuery.getRowCount(this);
        double leftCost = this.left.getRows();
        double rightCost = this.right.getRows();
        double maxCardinality = Math.max(leftCost, rightCost);
        double minCardinality = Math.min(leftCost, rightCost);
        if (Double.isInfinite(maxCardinality)
                || Double.isInfinite(minCardinality)) {
            rowCount = Double.MAX_VALUE;
        } else {
            rowCount = rowCount / (maxCardinality / minCardinality);
        }

        /*
         * // Joins can be flipped, and for many algorithms, both versions are
         * viable // and have the same cost. To make the results stable between
         * versions of // the planner, make one of the versions slightly more
         * expensive. switch (joinType) { case RIGHT: rowCount =
         * addEpsilon(rowCount); break; default: if (left.getId() >
         * right.getId()) { rowCount = addEpsilon(rowCount); } }
         */
        return planner.makeCost(rowCount, 0, 0);
    }
    
    private double addEpsilon(double d) {
        assert d >= 0d;
        final double d0 = d;
        if (d < 10) {
          // For small d, adding 1 would change the value significantly.
          d *= 1.001d;
          if (d != d0) {
            return d;
          }
        }
        // For medium d, add 1. Keeps integral values integral.
        ++d;
        if (d != d0) {
          return d;
        }
        // For large d, adding 1 might not change the value. Add .1%.
        // If d is NaN, this still will probably not change the value. That's OK.
        d *= 1.001d;
        return d;
      }
    
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      // We always "build" the
      double rowCount = RelMetadataQuery.getRowCount(this);

      // Joins can be flipped, and for many algorithms, both versions are viable
      // and have the same cost. To make the results stable between versions of
      // the planner, make one of the versions slightly more expensive.
      switch (joinType) {
      case RIGHT:
        rowCount = addEpsilon(rowCount);
        break;
      default:
        if (left.getId() > right.getId()) {
          rowCount = addEpsilon(rowCount);
        }
      }

      // Cheaper if the smaller number of rows is coming from the RHS.
      final double rightRowCount = right.getRows();
      final double leftRowCount = left.getRows();
      if (rightRowCount > leftRowCount && !Double.isInfinite(rightRowCount)) {
        rowCount *= rightRowCount / (leftRowCount + 1d);
      }
      if (condition.isAlwaysTrue()) {
        rowCount *= 10d;
      }
      return planner.makeCost(rowCount, 0, 0);
    }

    // @Override
    public double _getRows() {
        final double leftRowCount = left.getRows();
        final double rightRowCount = right.getRows();
        return leftRowCount * rightRowCount;
    }

    @Override
    public double getAvgTupleSize() {
        double leftAvgDataSz = OptiqUtil.getNonSubsetRelNode(left)
                .getAvgTupleSize();
        double rightAvgDataSz = OptiqUtil.getNonSubsetRelNode(right)
                .getAvgTupleSize();

        // TODO: subtract join keys that is not projected out
        return (leftAvgDataSz + rightAvgDataSz);
    }

    @Override
    public List<HiveColStat> getColStat(List<Integer> projIndxLst) {
        return OptiqStatsUtil.getJoinRelColStat(this, projIndxLst);
    }

    @Override
    public HiveColStat getColStat(Integer projIndx) {
        return OptiqStatsUtil.computeColStat(this, projIndx);
    }

    @Override
    public Double getEstimatedMemUsageInVertex() {
        return ((HiveRel) left).getEstimatedMemUsageInVertex()
                + ((HiveRel) right).getEstimatedMemUsageInVertex();
    }

    @Override
    public boolean propagateBucketingTraitUpwardsViaTransformation(
            List<Integer> bucketingCols, List<Integer> bucketSortCols) {
        return false;
    }

    @Override
    public boolean propagateSortingTraitUpwardsViaTransformation(
            List<Integer> sortingCols) {
        return false;
    }

    @Override
    public boolean shouldPropagateTraitFromChildViaTransformation(
            RelBucketing bucketTraitFromChild) {
        return false;
    }

    @Override
    public boolean shouldPropagateTraitFromChildViaTransformation(
            RelCollation sortTraitFromChild) {
        return false;
    }
}
