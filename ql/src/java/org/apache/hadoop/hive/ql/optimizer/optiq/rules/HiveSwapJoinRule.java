package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.rules.SwapJoinRule;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexNode;

import java.util.List;


public class HiveSwapJoinRule extends SwapJoinRule {
  public static final ProjectFactory HIVE_PROJECT_FACTORY =
      new HiveProjectFactory();

  public static final HiveSwapJoinRule INSTANCE = new HiveSwapJoinRule();

  private HiveSwapJoinRule() {
    super(HiveJoinRel.class, HIVE_PROJECT_FACTORY);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return super.matches(call)
        && call.<HiveJoinRel>rel(0).getJoinAlgorithm() == JoinAlgorithm.NONE;
  }

  /** Implementation of {@link ProjectFactory} that returns
   * {@link org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel}. */
  private static class HiveProjectFactory implements ProjectFactory {
    @Override
    public RelNode createProject(RelNode input, List<RexNode> exps,
        List<String> fieldNames) {
      RelNode project = HiveProjectRel.create(input, exps, fieldNames);

      // Make sure extra traits are carried over from the original rel
      project = RelOptRule.convert(project, input.getTraitSet());
      return project;
    }
  }
}
