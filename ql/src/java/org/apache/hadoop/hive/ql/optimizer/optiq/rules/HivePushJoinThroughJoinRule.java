package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import net.hydromatic.optiq.util.BitSets;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexPermuteInputsShuttle;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.util.mapping.Mappings;

public class HivePushJoinThroughJoinRule extends RelOptRule {
  private final boolean m_right;
  public static final RelOptRule RIGHT =
      new HivePushJoinThroughJoinRule("PushJoinThroughJoinRule:right", true, HiveJoinRel.class);
  public static final RelOptRule LEFT =
      new HivePushJoinThroughJoinRule("PushJoinThroughJoinRule:left", false, HiveJoinRel.class);

  private HivePushJoinThroughJoinRule(String description, boolean right,
      Class<? extends JoinRelBase> clazz) {
    super(
        some(
            clazz, any(clazz), any(HiveRel.class)),
        description);
    m_right = right;
  }


  @Override
  public boolean matches(RelOptRuleCall call)
  {
    boolean isAMatch = false;
    final HiveJoinRel topJoin = call.rel(0);
    final HiveJoinRel bottomJoin = call.rel(1);

    if (topJoin.getJoinAlgorithm() == JoinAlgorithm.NONE
        && bottomJoin.getJoinAlgorithm() == JoinAlgorithm.NONE) {
      isAMatch = true;
    }

    return isAMatch;
  }

//TODO: Migrate HiveProjectRel creation to parent class and remove everything below
  @Override
  public void onMatch(RelOptRuleCall call) {
    if (m_right) {
      onMatchRight(call);
    } else {
      onMatchLeft(call);
    }
  }

  private void onMatchRight(RelOptRuleCall call) {
    final JoinRelBase topJoin = call.rel(0);
    final JoinRelBase bottomJoin = call.rel(1);
    final RelNode relC = call.rel(2);
    final RelNode relA = bottomJoin.getLeft();
    final RelNode relB = bottomJoin.getRight();
    final RelOptCluster cluster = topJoin.getCluster();

    // topJoin
    // / \
    // bottomJoin C
    // / \
    // A B

    final int aCount = relA.getRowType().getFieldCount();
    final int bCount = relB.getRowType().getFieldCount();
    final int cCount = relC.getRowType().getFieldCount();
    final BitSet bBitSet = BitSets.range(aCount, aCount + bCount);

    // becomes
    //
    // newTopJoin
    // / \
    // newBottomJoin B
    // / \
    // A C

    // If either join is not inner, we cannot proceed.
    // (Is this too strict?)
    if (topJoin.getJoinType() != JoinRelType.INNER
        || bottomJoin.getJoinType() != JoinRelType.INNER)
    {
      return;
    }

    // Split the condition of topJoin into a conjunction. Each of the
    // parts that does not use columns from B can be pushed down.
    final List<RexNode> intersecting = new ArrayList<RexNode>();
    final List<RexNode> nonIntersecting = new ArrayList<RexNode>();
    split(topJoin.getCondition(), bBitSet, intersecting, nonIntersecting);

    // If there's nothing to push down, it's not worth proceeding.
    if (nonIntersecting.isEmpty()) {
      return;
    }

    // Split the condition of bottomJoin into a conjunction. Each of the
    // parts that use columns from B will need to be pulled up.
    final List<RexNode> bottomIntersecting = new ArrayList<RexNode>();
    final List<RexNode> bottomNonIntersecting = new ArrayList<RexNode>();
    split(
        bottomJoin.getCondition(), bBitSet, bottomIntersecting,
        bottomNonIntersecting);

    // target: | A | C |
    // source: | A | B | C |
    final Mappings.TargetMapping bottomMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            0, 0, aCount,
            aCount, aCount + bCount, cCount);
    List<RexNode> newBottomList = new ArrayList<RexNode>();
    new RexPermuteInputsShuttle(bottomMapping, relA, relC)
        .visitList(nonIntersecting, newBottomList);
    final Mappings.TargetMapping bottomBottomMapping =
        Mappings.createShiftMapping(
            aCount + bCount,
            0, 0, aCount);
    new RexPermuteInputsShuttle(bottomBottomMapping, relA, relC)
        .visitList(bottomNonIntersecting, newBottomList);
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode newBottomCondition =
        RexUtil.composeConjunction(rexBuilder, newBottomList, false);
    final JoinRelBase newBottomJoin =
        bottomJoin.copy(
            bottomJoin.getTraitSet(), newBottomCondition, relA, relC);

    // target: | A | C | B |
    // source: | A | B | C |
    final Mappings.TargetMapping topMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            0, 0, aCount,
            aCount + cCount, aCount, bCount,
            aCount, aCount + bCount, cCount);
    List<RexNode> newTopList = new ArrayList<RexNode>();
    new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB)
        .visitList(intersecting, newTopList);
    new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB)
        .visitList(bottomIntersecting, newTopList);
    RexNode newTopCondition =
        RexUtil.composeConjunction(rexBuilder, newTopList, false);
    @SuppressWarnings("SuspiciousNameCombination")
    final JoinRelBase newTopJoin =
        topJoin.copy(
            topJoin.getTraitSet(), newTopCondition, newBottomJoin, relB);

    assert !Mappings.isIdentity(topMapping);
    final RelNode newProject = OptiqUtil.createProjectIfNeeded((HiveRel) newTopJoin,
        Mappings.asList(topMapping));

    call.transformTo(newProject);
  }

  /**
   * Similar to {@link #onMatch}, but swaps the upper sibling with the left
   * of the two lower siblings, rather than the right.
   */
  private void onMatchLeft(RelOptRuleCall call) {
    final JoinRelBase topJoin = call.rel(0);
    final JoinRelBase bottomJoin = call.rel(1);
    final RelNode relC = call.rel(2);
    final RelNode relA = bottomJoin.getLeft();
    final RelNode relB = bottomJoin.getRight();
    final RelOptCluster cluster = topJoin.getCluster();

    // topJoin
    // / \
    // bottomJoin C
    // / \
    // A B

    final int aCount = relA.getRowType().getFieldCount();
    final int bCount = relB.getRowType().getFieldCount();
    final int cCount = relC.getRowType().getFieldCount();
    final BitSet aBitSet = BitSets.range(aCount);

    // becomes
    //
    // newTopJoin
    // / \
    // newBottomJoin A
    // / \
    // C B

    // If either join is not inner, we cannot proceed.
    // (Is this too strict?)
    if (topJoin.getJoinType() != JoinRelType.INNER
        || bottomJoin.getJoinType() != JoinRelType.INNER)
    {
      return;
    }

    // Split the condition of topJoin into a conjunction. Each of the
    // parts that does not use columns from A can be pushed down.
    final List<RexNode> intersecting = new ArrayList<RexNode>();
    final List<RexNode> nonIntersecting = new ArrayList<RexNode>();
    split(topJoin.getCondition(), aBitSet, intersecting, nonIntersecting);

    // If there's nothing to push down, it's not worth proceeding.
    if (nonIntersecting.isEmpty()) {
      return;
    }

    // Split the condition of bottomJoin into a conjunction. Each of the
    // parts that use columns from B will need to be pulled up.
    final List<RexNode> bottomIntersecting = new ArrayList<RexNode>();
    final List<RexNode> bottomNonIntersecting = new ArrayList<RexNode>();
    split(
        bottomJoin.getCondition(), aBitSet, bottomIntersecting,
        bottomNonIntersecting);

    // target: | C | B |
    // source: | A | B | C |
    final Mappings.TargetMapping bottomMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            cCount, aCount, bCount,
            0, aCount + bCount, cCount);
    List<RexNode> newBottomList = new ArrayList<RexNode>();
    new RexPermuteInputsShuttle(bottomMapping, relC, relB)
        .visitList(nonIntersecting, newBottomList);
    final Mappings.TargetMapping bottomBottomMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            0, aCount + bCount, cCount,
            cCount, aCount, bCount);
    new RexPermuteInputsShuttle(bottomBottomMapping, relC, relB)
        .visitList(bottomNonIntersecting, newBottomList);
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode newBottomCondition =
        RexUtil.composeConjunction(rexBuilder, newBottomList, false);
    final JoinRelBase newBottomJoin =
        bottomJoin.copy(
            bottomJoin.getTraitSet(), newBottomCondition, relC, relB);

    // target: | C | B | A |
    // source: | A | B | C |
    final Mappings.TargetMapping topMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            cCount + bCount, 0, aCount,
            cCount, aCount, bCount,
            0, aCount + bCount, cCount);
    List<RexNode> newTopList = new ArrayList<RexNode>();
    new RexPermuteInputsShuttle(topMapping, newBottomJoin, relA)
        .visitList(intersecting, newTopList);
    new RexPermuteInputsShuttle(topMapping, newBottomJoin, relA)
        .visitList(bottomIntersecting, newTopList);
    RexNode newTopCondition =
        RexUtil.composeConjunction(rexBuilder, newTopList, false);
    @SuppressWarnings("SuspiciousNameCombination")
    final JoinRelBase newTopJoin =
        topJoin.copy(
            topJoin.getTraitSet(), newTopCondition, newBottomJoin, relA);

    final RelNode newProject = OptiqUtil.createProjectIfNeeded((HiveRel) newTopJoin,
        Mappings.asList(topMapping));

    call.transformTo(newProject);
  }

  /**
   * Splits a condition into conjunctions that do or do not intersect with
   * a given bit set.
   */
  static void split(
      RexNode condition,
      BitSet bitSet,
      List<RexNode> intersecting,
      List<RexNode> nonIntersecting)
  {
    for (RexNode node : RelOptUtil.conjunctions(condition)) {
      BitSet inputBitSet = RelOptUtil.InputFinder.bits(node);
      if (bitSet.intersects(inputBitSet)) {
        intersecting.add(node);
      } else {
        nonIntersecting.add(node);
      }
    }
  }

  /*
   * @Override
   * public boolean matches(RelOptRuleCall call)
   * {
   * boolean isAMatch = false;
   * final HiveJoinRel topJoin = call.rel(0);
   * final HiveJoinRel bottomJoin = call.rel(1);
   *
   * if (!topJoin.partOfMultiJoin() && !bottomJoin.partOfMultiJoin()) {
   * RelNode rel1 = topJoin.getRight();
   * RelNode rel2 = (m_right ? bottomJoin.getRight() : bottomJoin.getLeft());
   * if (isMultiJoinSafe(rel1) && isMultiJoinSafe(rel2)) {
   * isAMatch = true;
   * }
   * }
   *
   * return isAMatch;
   * }
   *
   * private boolean isMultiJoinSafe(RelNode n) {
   * boolean multiJoinSafe = false;
   *
   * if (n instanceof HiveJoinRel) {
   * HiveJoinRel j = (HiveJoinRel) n;
   * if (j.partOfMultiJoin() && j.getContainerMultiJoinRel().isTopJoin(j)) {
   * multiJoinSafe = true;
   * }
   * }else {
   * multiJoinSafe = true;
   * }
   *
   * return multiJoinSafe;
   * }
   */
}
