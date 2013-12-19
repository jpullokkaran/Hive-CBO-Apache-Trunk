package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveLimitRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.volcano.RelSubset;

public class PropagateSortTraitUpwardsRule extends RelOptRule {
  public static final RelOptRule FILTER =
      new PropagateSortTraitUpwardsRule(
          "PropagateSortTraitUpwardsRule:FILTER", HiveFilterRel.class);
  public static final RelOptRule LIMIT =
      new PropagateSortTraitUpwardsRule(
          "PropagateSortTraitUpwardsRule:LIMIT", HiveLimitRel.class);
  public static final RelOptRule PROJECT =
      new PropagateSortTraitUpwardsRule(
          "PropagateSortTraitUpwardsRule:PROJECT", HiveProjectRel.class);

  protected PropagateSortTraitUpwardsRule(String description,
      Class<? extends SingleRel> clazz) {
    super(some(clazz, any(RelSubset.class)));
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    HiveRel parentRel = (HiveRel) call.rels[0];
    RelSubset relSubSet = (RelSubset) call.rels[1];
    RelCollation sortTraitFromChild = OptiqTraitsUtil.getSortTrait(relSubSet.getTraitSet());

    if (sortTraitFromChild != null
        && parentRel.shouldPropagateTraitFromChildViaTransformation(sortTraitFromChild)) {
      parentRel.propagateBucketingTraitUpwardsViaTransformation(null, null);
      return true;
    }

    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveRel parentRel = (HiveRel) call.rels[0];
    RelSubset relSubSet = (RelSubset) call.rels[1];
    RelCollation sortTraitFromChild = OptiqTraitsUtil.getSortTrait(relSubSet.getTraitSet());

    HiveRel newParentRel = (HiveRel) parentRel.copy(
        parentRel.getTraitSet().plus(sortTraitFromChild), parentRel.getInputs());
    call.getPlanner().ensureRegistered(newParentRel, parentRel);
  }
}
