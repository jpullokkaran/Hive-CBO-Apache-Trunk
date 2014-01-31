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
    super(operand(HiveJoinRel.class,
        operand(HiveRel.class, any()),
        operand(HiveRel.class, any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveJoinRel join = call.rel(0);
    return join.getJoinAlgorithm() == JoinAlgorithm.NONE;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveJoinRel j = call.rel(0);
    final RelNode leftNode = call.rel(1);
    final RelNode rightNode = call.rel(2);
    final JoinPredicateInfo jpi = j.getJoinPredicateInfo();
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
      RelTraitSet shuffleJoinTrait = OptiqTraitsUtil.getShuffleJoinTraitSet(newJoin);
      // REVIEW: merge result ignored
      newJoin.getTraitSet().merge(shuffleJoinTrait);
      call.getPlanner().ensureRegistered(newJoin, j);
    }
  }
}
