package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.OptiqStatsUtil;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.UnionRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

public class HiveUnionRel extends UnionRelBase implements HiveRel {

    public HiveUnionRel(RelOptCluster cluster, RelTraitSet traitSet,
            List<RelNode> inputs, boolean all) {
        super(cluster, OptiqTraitsUtil.getHiveTraitSet(cluster, traitSet),
                inputs, all);
    }

    @Override
    public HiveUnionRel copy(RelTraitSet traitSet, List<RelNode> inputs,
            boolean all) {
        return new HiveUnionRel(getCluster(), traitSet, inputs, all);
    }

    public void implement(Implementor implementor) {
    }

    @Override
    public double getAvgTupleSize() {
        return OptiqStatsUtil.computeUnionRelAvgTupleSize(inputs);
    }

    @Override
    public List<HiveColStat> getColStat(List<Integer> projIndxLst) {
        return OptiqStatsUtil.computeUnionRelColStat(inputs, projIndxLst);
    }

    @Override
    public HiveColStat getColStat(Integer projIndx) {
        return OptiqStatsUtil.computeColStat(this, projIndx);
    }

    @Override
    public Double getEstimatedMemUsageInVertex() {
        double totalMemUsage = 0;
        for (RelNode r : inputs) {
            totalMemUsage += OptiqUtil.getNonSubsetRelNode(r)
                    .getEstimatedMemUsageInVertex();
        }

        return totalMemUsage;
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
