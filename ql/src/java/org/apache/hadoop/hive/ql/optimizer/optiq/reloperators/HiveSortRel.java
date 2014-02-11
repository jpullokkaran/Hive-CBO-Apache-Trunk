package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class HiveSortRel extends SortRel implements HiveRel {

    public HiveSortRel(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, OptiqTraitsUtil.getSortTraitSet(cluster, traitSet,
                collation), child, collation, offset, fetch);
        //assert getConvention() instanceof HiveRel;
        assert getConvention() == child.getConvention();
    }

    @Override
    public HiveSortRel copy(RelTraitSet traitSet, RelNode newInput,
            RelCollation newCollation, RexNode offset, RexNode fetch) {
        // TODO: can we blindly copy sort trait? What if inputs changed and we
        // are now sorting by
        // different cols
        return new HiveSortRel(getCluster(), traitSet, newInput, newCollation,
                offset, fetch);
    }
    
    public RexNode getFetchExpr() {
    	return fetch;
    }

    public void implement(Implementor implementor) {
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
