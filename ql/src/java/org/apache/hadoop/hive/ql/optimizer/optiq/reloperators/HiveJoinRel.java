package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCostUtil;
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
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlOperator;

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
        List<RelDataTypeField> sysFieldList = new LinkedList<RelDataTypeField>();
        final List<RexNode> leftKeys = new ArrayList<RexNode>();
        final List<RexNode> rightKeys = new ArrayList<RexNode>();
        List<Integer> filterNulls = new LinkedList<Integer>();
        RexNode remaining = RelOptUtil.splitJoinCondition(sysFieldList, left, right,
                condition, leftKeys, rightKeys, filterNulls, (List<SqlOperator>) null);

        if (remaining != null && !remaining.isAlwaysTrue()) {
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

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
    	return HiveCostUtil.computeCost(this);
    }

    @Override
    public double getRows() {
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
