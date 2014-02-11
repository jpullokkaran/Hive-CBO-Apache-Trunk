package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveLimitRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.RelNode;
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
    super(operand(clazz, operand(RelSubset.class, any())), description);
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    HiveRel parentRel = call.rel(0);
    RelSubset relSubSet = call.rel(1);
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
    HiveRel parentRel = call.rel(0);
    RelNode child = call.rel(1);
    RelBucketing bucketTraitFromChild =
        OptiqTraitsUtil.getBucketingTrait(child.getTraitSet());

    HiveRel newParentRel = (HiveRel) parentRel.copy(
        parentRel.getTraitSet().plus(bucketTraitFromChild), parentRel.getInputs());
    // REVIEW: should be transformTo?
    call.getPlanner().ensureRegistered(newParentRel, parentRel);
  }
}