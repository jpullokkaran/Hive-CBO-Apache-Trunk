package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil.JoinPredicateInfoOld;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class ConvertToSMBJoinRule extends RelOptRule {

  public ConvertToSMBJoinRule() {
	  super(operand(HiveJoinRel.class, operand(HiveRel.class, any()),
				operand(HiveRel.class, any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    boolean matchesRule = false;

    final HiveJoinRel j = call.rel(0);
    final HiveRel left = OptiqUtil.getNonSubsetRelNode(j.getLeft());
    final HiveRel right = OptiqUtil.getNonSubsetRelNode(j.getRight());
    if (j.getJoinAlgorithm() != JoinAlgorithm.NONE) {
      return false;
    }
    RelBucketing leftBucketingTrait = OptiqTraitsUtil.getBucketingTrait(left.getTraitSet());
    RelBucketing rightBucketingTrait = OptiqTraitsUtil.getBucketingTrait(right.getTraitSet());
    if (leftBucketingTrait == null
        || rightBucketingTrait == null
        || !leftBucketingTrait.noOfBucketsMultipleOfEachOther(rightBucketingTrait)) {
      return false;
    }
    final JoinPredicateInfoOld jpi = j.getJoinPredicateInfoOld();
    if (leftBucketingTrait.getPartitionCols().equals(jpi.getJoinKeysFromLeftRelation())
        && rightBucketingTrait.getPartitionCols().equals(jpi.getJoinKeysFromRightRelation())
        && jpi.getNonJoinKeyLeafPredicates().isEmpty()) {
      if (leftBucketingTrait.getSortingCols().equals(jpi.getJoinKeysFromLeftRelation())
          && rightBucketingTrait.getSortingCols().equals(jpi.getJoinKeysFromRightRelation())) {
        matchesRule = true;
      }
    }

    return matchesRule;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveJoinRel j = call.rel(0);
    HiveJoinRel newJoin = j.copy(j.getTraitSet(), j.getCondition(), j.getLeft(), j.getRight(), JoinAlgorithm.SMB_JOIN,
        HiveJoinRel.MapJoinStreamingRelation.NONE);
    RelTraitSet additionalTraits = OptiqTraitsUtil.getBucketJoinTraitSet(j);
    // REVIEW: merge result ignored
    j.getTraitSet().merge(additionalTraits);
    // REVIEW: should be transformTo?
    call.getPlanner().ensureRegistered(newJoin, j);
  }
}
