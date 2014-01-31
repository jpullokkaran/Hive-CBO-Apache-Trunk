package org.apache.hadoop.hive.ql.optimizer.optiq.cost;

import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.rules.java.JavaRules;

import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.rel.rules.MergeProjectRule;
import org.eigenbase.rel.rules.PushFilterPastJoinRule;
import org.eigenbase.rel.rules.PushFilterPastProjectRule;
import org.eigenbase.rel.rules.PushJoinThroughJoinRule;
import org.eigenbase.rel.rules.PushSortPastProjectRule;
import org.eigenbase.rel.rules.ReduceAggregatesRule;
import org.eigenbase.rel.rules.RemoveDistinctAggregateRule;
import org.eigenbase.rel.rules.SwapJoinRule;
import org.eigenbase.rel.rules.TableAccessRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.ConventionTraitDef;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.volcano.RelSubset;
import org.eigenbase.relopt.volcano.VolcanoPlanner;


/*
 * hb: have to go down this path because choice was made to define HiveCost.
 * So references in VolcanoPlanner to VolcanoCost have to be overridden.
 */
public class HiveVolcanoPlanner extends VolcanoPlanner {
	
	/*
	 * Copied from
	 */
	private static final boolean ENABLE_COLLATION_TRAIT = true;

	public static  RelOptPlanner createPlanner() {
		final VolcanoPlanner planner = new HiveVolcanoPlanner();
		planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
		if (ENABLE_COLLATION_TRAIT) {
			planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
			planner.registerAbstractRelationalRules();
		}
		RelOptUtil.registerAbstractRels(planner);
		planner.addRule(JavaRules.ENUMERABLE_JOIN_RULE);
		planner.addRule(JavaRules.ENUMERABLE_PROJECT_RULE);
		planner.addRule(JavaRules.ENUMERABLE_FILTER_RULE);
		planner.addRule(JavaRules.ENUMERABLE_AGGREGATE_RULE);
		planner.addRule(JavaRules.ENUMERABLE_SORT_RULE);
		planner.addRule(JavaRules.ENUMERABLE_LIMIT_RULE);
		planner.addRule(JavaRules.ENUMERABLE_UNION_RULE);
		planner.addRule(JavaRules.ENUMERABLE_INTERSECT_RULE);
		planner.addRule(JavaRules.ENUMERABLE_MINUS_RULE);
		planner.addRule(JavaRules.ENUMERABLE_TABLE_MODIFICATION_RULE);
		planner.addRule(JavaRules.ENUMERABLE_VALUES_RULE);
		planner.addRule(JavaRules.ENUMERABLE_WINDOW_RULE);
		planner.addRule(JavaRules.ENUMERABLE_ONE_ROW_RULE);
		planner.addRule(TableAccessRule.INSTANCE);
		planner.addRule(MergeProjectRule.INSTANCE);
		planner.addRule(PushFilterPastProjectRule.INSTANCE);
		planner.addRule(PushFilterPastJoinRule.FILTER_ON_JOIN);
		planner.addRule(RemoveDistinctAggregateRule.INSTANCE);
		planner.addRule(ReduceAggregatesRule.INSTANCE);
		planner.addRule(SwapJoinRule.INSTANCE);
		planner.addRule(PushJoinThroughJoinRule.RIGHT);
		planner.addRule(PushJoinThroughJoinRule.LEFT);
		planner.addRule(PushSortPastProjectRule.INSTANCE);
		OptiqPrepare.Dummy.getSparkHandler().registerRules(planner);
		return planner;
	}

	public RelOptCost getCost(RelNode rel)
    {
        assert rel != null : "pre-condition: rel != null";
        if (rel instanceof RelSubset) {
            return super.getCost(rel);
        }
        if (rel.getTraitSet().getTrait(0) == Convention.NONE) {
            return makeInfiniteCost();
        }
        RelOptCost cost = RelMetadataQuery.getNonCumulativeCost(rel);
        if (!HiveCost.ZERO.isLt(cost)) {
            // cost must be positive, so nudge it
            cost = makeTinyCost();
        }
        for (RelNode input : rel.getInputs()) {
            cost = cost.plus(getCost(input));
        }
        return cost;
    }
	
	// implement Planner
    public RelOptCost makeCost(
        double dRows,
        double dCpu,
        double dIo)
    {
        return new HiveCost(dRows, dCpu, dIo);
    }

    public RelOptCost makeHugeCost()
    {
        return HiveCost.HUGE;
    }

    public RelOptCost makeInfiniteCost()
    {
        return HiveCost.INFINITY;
    }

    public RelOptCost makeTinyCost()
    {
        return HiveCost.TINY;
    }

    public RelOptCost makeZeroCost()
    {
        return HiveCost.ZERO;
    }
	
}
