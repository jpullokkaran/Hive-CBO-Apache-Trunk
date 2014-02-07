package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil.JoinPredicateInfoOld;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.MapJoinStreamingRelation;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class ConvertToBucketJoinRule extends RelOptRule {
  private final double m_maxMemorySize;

  public ConvertToBucketJoinRule(double maxAllowedSize) {
		super(operand(HiveJoinRel.class, operand(HiveRel.class, any()),
				operand(HiveRel.class, any())));
    m_maxMemorySize = maxAllowedSize;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    boolean matchesRule = false;

    HiveJoinRel j = call.rel(0);
    HiveRel left = OptiqUtil.getNonSubsetRelNode(j.getLeft());
    HiveRel right = OptiqUtil.getNonSubsetRelNode(j.getRight());
    if (j.getJoinAlgorithm() == JoinAlgorithm.NONE) {
      RelBucketing leftBucketingTrait = OptiqTraitsUtil.getBucketingTrait(left.getTraitSet());
      RelBucketing rightBucketingTrait = OptiqTraitsUtil.getBucketingTrait(right.getTraitSet());
      if (leftBucketingTrait != null && rightBucketingTrait != null
          && leftBucketingTrait.noOfBucketsMultipleOfEachOther(rightBucketingTrait)) {
        final JoinPredicateInfoOld jpi = j.getJoinPredicateInfoOld();
        if (leftBucketingTrait.getPartitionCols().equals(jpi.getJoinKeysFromLeftRelation())
            && rightBucketingTrait.getPartitionCols().equals(jpi.getJoinKeysFromRightRelation())
            && jpi.getNonJoinKeyLeafPredicates().isEmpty()) {
          matchesRule = true;
        }
      }
    }

    return matchesRule;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveJoinRel j = call.rel(0);
    MapJoinStreamingRelation streamingSide = getStreamingSide(j);
    if (streamingSide != null) {
      RelTraitSet additionalTraits = OptiqTraitsUtil.getBucketJoinTraitSet(j);
      // REVIEW: merge result ignored
      RelTraitSet newTraitSet = j.getTraitSet().merge(additionalTraits);
      HiveJoinRel newJoin = j.copy(newTraitSet, j.getCondition(), j.getLeft(),
          j.getRight(), JoinAlgorithm.BUCKET_JOIN, streamingSide);
      call.transformTo(newJoin);
    }
  }

  /**
   * TODO:
   * Since in Bucket Join case (unlike mapjoin) both relations are partitioned on join keys and only
   * matching buckets is presented to a parallel instance of BucketJoin, we don't really need to
   * worry about join direction (Outer VS. Inner) while selecting streaming side (assuming all the
   * entries in the HT come with null padded and values gets changed as it gets matched).
   *
   * @param j
   * @return
   */
  private MapJoinStreamingRelation getStreamingSide(HiveJoinRel j) {
    MapJoinStreamingRelation streamingSide = null;

    if (j.getJoinType() != JoinRelType.FULL) {
      HiveRel left = (HiveRel) OptiqUtil.getNonSubsetRelNode(j.getLeft());
      HiveRel right = (HiveRel) OptiqUtil.getNonSubsetRelNode(j.getRight());
      RelBucketing leftBucketingTrait = OptiqTraitsUtil.getBucketingTrait(left.getTraitSet());
      RelBucketing rightBucketingTrait = OptiqTraitsUtil.getBucketingTrait(right.getTraitSet());
      Double maxLeftBucketSz = leftBucketingTrait.getSizeOfLargestBucket();
      Double maxRightBucketSz = rightBucketingTrait.getSizeOfLargestBucket();
      Double leftMemUsageSoFar = left.getEstimatedMemUsageInVertex();
      Double rightMemUsageSoFar = right.getEstimatedMemUsageInVertex();

      if (j.getJoinType() == JoinRelType.LEFT) {
        if (((leftMemUsageSoFar + maxRightBucketSz) < m_maxMemorySize)) {
          streamingSide = MapJoinStreamingRelation.LEFT_RELATION;
        }
      } else if (j.getJoinType() == JoinRelType.RIGHT) {
        if ((rightMemUsageSoFar + maxLeftBucketSz) < m_maxMemorySize) {
          streamingSide = MapJoinStreamingRelation.RIGHT_RELATION;
        }
      } else {
        if ((maxLeftBucketSz <= maxRightBucketSz)
            && ((leftMemUsageSoFar + maxRightBucketSz) < m_maxMemorySize)) {
          streamingSide = MapJoinStreamingRelation.LEFT_RELATION;
        } else if ((rightMemUsageSoFar + maxLeftBucketSz) < m_maxMemorySize) {
          streamingSide = MapJoinStreamingRelation.RIGHT_RELATION;
        }
      }
    }

    return streamingSide;
  }
}
