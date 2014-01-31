package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.MapJoinStreamingRelation;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;

public class ConvertToMapJoinRule extends RelOptRule {
  private final double m_maxMemorySize;

  public ConvertToMapJoinRule(double maxAllowedSize) {
    super(operand(HiveJoinRel.class, operand(RelNode.class, any()), operand(RelNode.class, any())));
    m_maxMemorySize = maxAllowedSize;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveJoinRel j = call.rel(0);
    return j.getJoinAlgorithm() == JoinAlgorithm.NONE;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveJoinRel j = call.rel(0);
    final MapJoinStreamingRelation streamingSide = getStreamingSide(j);
    if (streamingSide != null) {
      HiveJoinRel newJoin =
          OptiqUtil.introduceBroadcastOperator(j, streamingSide);
      call.transformTo(newJoin);
    }
  }


  /*
   * NOTE:
   * 1. Streaming side has to be outer side for one sided outer join
   * 2. Map Join is not supported for full outer join
   */
  private MapJoinStreamingRelation getStreamingSide(HiveJoinRel j) {
    MapJoinStreamingRelation streamingSide = null;

    //TODO: Simplify code
    if (j.getJoinType() != JoinRelType.FULL) {
      HiveRel left = OptiqUtil.getNonSubsetRelNode(j.getLeft());
      HiveRel right = OptiqUtil.getNonSubsetRelNode(j.getRight());
      double leftSzToKeepInMem = left.getAvgTupleSize() * left.getRows();
      double rightSzToKeepInMem = right.getAvgTupleSize() * right.getRows();
      double leftMemUsageSoFar = left.getEstimatedMemUsageInVertex();
      double rightMemUsageSoFar = right.getEstimatedMemUsageInVertex();
      boolean leftIsPartOfMapJoinAndISStreamed = false;
      boolean rightIsPartOfMapJoinAndISStreamed = false;

      if (j.getJoinType() == JoinRelType.LEFT) {
        if ((leftMemUsageSoFar + rightSzToKeepInMem) < m_maxMemorySize) {
          streamingSide = MapJoinStreamingRelation.LEFT_RELATION;
        }
      }
      else if (j.getJoinType() == JoinRelType.RIGHT) {
        if ((rightMemUsageSoFar + leftSzToKeepInMem) < m_maxMemorySize) {
          streamingSide = MapJoinStreamingRelation.RIGHT_RELATION;
        }
      } else {
        leftIsPartOfMapJoinAndISStreamed = OptiqUtil.isAlreadyStreamingWithinSameVertex(left);
        rightIsPartOfMapJoinAndISStreamed = OptiqUtil.isAlreadyStreamingWithinSameVertex(right);
        if (leftIsPartOfMapJoinAndISStreamed && !rightIsPartOfMapJoinAndISStreamed) {
          streamingSide = MapJoinStreamingRelation.LEFT_RELATION;
        } else if (rightIsPartOfMapJoinAndISStreamed && !leftIsPartOfMapJoinAndISStreamed) {
          streamingSide = MapJoinStreamingRelation.RIGHT_RELATION;
        } else {
          if ((leftSzToKeepInMem <= rightSzToKeepInMem)
              && (leftSzToKeepInMem + rightMemUsageSoFar) < m_maxMemorySize) {
            streamingSide = MapJoinStreamingRelation.RIGHT_RELATION;
          } else if ((rightSzToKeepInMem + leftMemUsageSoFar) < m_maxMemorySize) {
            streamingSide = MapJoinStreamingRelation.LEFT_RELATION;
          }
        }
      }
    }

    return streamingSide;
  }
}
