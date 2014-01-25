package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class ConvertToCommonJoinRule extends RelOptRule {

  public ConvertToCommonJoinRule() {
    super(operand(HiveJoinRel.class, operand(HiveRel.class, any()), operand(HiveRel.class, any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    if (((HiveJoinRel) call.rels[0]).getJoinAlgorithm() == JoinAlgorithm.NONE) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveJoinRel j = (HiveJoinRel) call.rels[0];
    RelNode leftNode = call.rels[1];
    RelNode rightNode = call.rels[2];
    JoinPredicateInfo jpi = OptiqUtil.getJoinPredicateInfo(j);
    RelTraitSet leftTraitSet = leftNode.getTraitSet();
    RelTraitSet rightTraitSet = rightNode.getTraitSet();
    RelBucketing leftBucketingTrait = OptiqTraitsUtil.getBucketingTrait(leftTraitSet);
    RelBucketing rightBucketingTrait = OptiqTraitsUtil.getBucketingTrait(rightTraitSet);
    boolean introduceShuffleAtLeft = false;
    boolean introduceShuffleAtRight = false;

    if (leftBucketingTrait == null
        || !leftBucketingTrait.getPartitionCols().equals(jpi.getJoinKeysFromLeftRelation())
        || leftBucketingTrait.getSizeOfLargestBucket() > (0.0)) {
      introduceShuffleAtLeft = true;
      OptiqTraitsUtil.requestBucketTraitPropagationViaTransformation((HiveRel) leftNode,
          jpi.getJoinKeysFromLeftRelation(), jpi.getJoinKeysFromLeftRelation());
    }

    if (rightBucketingTrait == null
        || !rightBucketingTrait.getPartitionCols().equals(jpi.getJoinKeysFromRightRelation())
        || rightBucketingTrait.getSizeOfLargestBucket() > (0.0)) {
      introduceShuffleAtRight = true;
      OptiqTraitsUtil.requestBucketTraitPropagationViaTransformation((HiveRel) rightNode,
          jpi.getJoinKeysFromRightRelation(), jpi.getJoinKeysFromRightRelation());
    }

    if (introduceShuffleAtLeft || introduceShuffleAtRight) {
      HiveJoinRel newJoin = OptiqUtil.introduceShuffleOperator(j, introduceShuffleAtLeft,
          introduceShuffleAtRight,
          jpi.getJoinKeysFromLeftRelation(), jpi.getJoinKeysFromLeftRelation(), call.getPlanner());
      newJoin.setJoinAlgorithm(JoinAlgorithm.COMMON_JOIN);
      RelTraitSet shuffleJoinTrait = OptiqTraitsUtil.getShuffleJoinTraitSet(newJoin);
      newJoin.getTraitSet().merge(shuffleJoinTrait);
      call.getPlanner().ensureRegistered(newJoin, j);
    }
  }
}
