package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class ConvertToSMBJoinRule extends RelOptRule {

  public ConvertToSMBJoinRule() {
    super(some(HiveJoinRel.class, any(HiveRel.class), any(HiveRel.class)));
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    boolean matchesRule = false;

    HiveJoinRel j = (HiveJoinRel) call.rels[0];
    HiveRel left = (HiveRel) j.getLeft();
    HiveRel right = (HiveRel) j.getRight();
    if (j.getJoinAlgorithm() == JoinAlgorithm.NONE) {
      RelBucketing leftBucketingTrait = OptiqTraitsUtil.getBucketingTrait(left.getTraitSet());
      RelBucketing rightBucketingTrait = OptiqTraitsUtil.getBucketingTrait(right.getTraitSet());
      if (leftBucketingTrait != null && rightBucketingTrait != null
          && leftBucketingTrait.noOfBucketsMultipleOfEachOther(rightBucketingTrait)) {
        JoinPredicateInfo jpi = OptiqUtil.getJoinPredicateInfo(j);
        if (leftBucketingTrait.getPartitionCols().equals(jpi.getJoinKeysFromLeftRelation())
            && rightBucketingTrait.getPartitionCols().equals(jpi.getJoinKeysFromRightRelation())
            && (jpi.getNonJoinKeyLeafPredicates() == null || jpi.getNonJoinKeyLeafPredicates()
                .isEmpty())) {
          if (leftBucketingTrait.getSortingCols().equals(jpi.getJoinKeysFromLeftRelation())
              && rightBucketingTrait.getSortingCols().equals(jpi.getJoinKeysFromRightRelation())) {
            matchesRule = true;
          }
        }
      }
    }

    return matchesRule;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveJoinRel j = (HiveJoinRel) call.rels[0];
    HiveJoinRel newJoin = (HiveJoinRel) j.copy(j.getTraitSet(), j.getInputs());
    j.setJoinAlgorithm(JoinAlgorithm.SMB_JOIN);
    RelTraitSet additionalTraits = OptiqTraitsUtil.getBucketJoinTraitSet(j);
    j.getTraitSet().merge(additionalTraits);
    call.getPlanner().ensureRegistered(newJoin, j);
  }
}
