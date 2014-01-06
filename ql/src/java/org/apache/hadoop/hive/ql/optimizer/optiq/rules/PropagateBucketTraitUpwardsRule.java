package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveLimitRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.volcano.RelSubset;

public class PropagateBucketTraitUpwardsRule extends RelOptRule {
  public static final RelOptRule FILTER =
      new PropagateBucketTraitUpwardsRule(
          "PropagateBucketTraitUpwardsRule:FILTER", HiveFilterRel.class);
  public static final RelOptRule LIMIT =
      new PropagateBucketTraitUpwardsRule(
          "PropagateBucketTraitUpwardsRule:LIMIT", HiveLimitRel.class);
  public static final RelOptRule PROJECT =
      new PropagateBucketTraitUpwardsRule(
          "PropagateBucketTraitUpwardsRule:PROJECT", HiveProjectRel.class);

  protected PropagateBucketTraitUpwardsRule(String description,
      Class<? extends SingleRel> clazz) {
    super(some(clazz, any(RelSubset.class)), description);
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    HiveRel parentRel = (HiveRel) call.rels[0];
    RelSubset relSubSet = (RelSubset) call.rels[1];
    RelBucketing bucketTraitFromChild = OptiqTraitsUtil.getBucketingTrait(relSubSet.getTraitSet());

    if (bucketTraitFromChild != null
        && parentRel.shouldPropagateTraitFromChildViaTransformation(bucketTraitFromChild)) {
      parentRel.propagateBucketingTraitUpwardsViaTransformation(null, null);
      return true;
    }

    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveRel parentRel = (HiveRel) call.rels[0];
    RelSubset relSubSet = (RelSubset) call.rels[1];
    RelBucketing bucketTraitFromChild = OptiqTraitsUtil.getBucketingTrait(relSubSet.getTraitSet());

    HiveRel newParentRel = (HiveRel) parentRel.copy(
        parentRel.getTraitSet().plus(bucketTraitFromChild), parentRel.getInputs());
    call.getPlanner().ensureRegistered(newParentRel, parentRel);
  }
}
