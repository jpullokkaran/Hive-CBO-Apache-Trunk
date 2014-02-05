package org.apache.hadoop.hive.ql.optimizer.optiq.cost;

import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.rules.java.JavaRules;

import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.rules.MergeProjectRule;
import org.eigenbase.rel.rules.PushFilterPastJoinRule;
import org.eigenbase.rel.rules.PushFilterPastProjectRule;
import org.eigenbase.rel.rules.PushJoinThroughJoinRule;
import org.eigenbase.rel.rules.PushSortPastProjectRule;
import org.eigenbase.rel.rules.ReduceAggregatesRule;
import org.eigenbase.rel.rules.RemoveDistinctAggregateRule;
import org.eigenbase.rel.rules.SwapJoinRule;
import org.eigenbase.rel.rules.TableAccessRule;
import org.eigenbase.relopt.ConventionTraitDef;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.volcano.VolcanoPlanner;

/**
 * Refinement of {@link org.eigenbase.relopt.volcano.VolcanoPlanner} for Hive.
 *
 * <p>It uses uses
 * {@link org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost} as its
 * cost model.
 */
public class HiveVolcanoPlanner extends VolcanoPlanner {
	private static final boolean ENABLE_COLLATION_TRAIT = true;

  /** Creates a HiveVolcanoPlanner. */
  public HiveVolcanoPlanner() {
    super(HiveCost.FACTORY);
  }

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
}
