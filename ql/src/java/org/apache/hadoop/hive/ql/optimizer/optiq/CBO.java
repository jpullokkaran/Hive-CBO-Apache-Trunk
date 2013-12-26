package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.tools.Frameworks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.ConvertToBucketJoinRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.ConvertToCommonJoinRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.ConvertToMapJoinRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.ConvertToSMBJoinRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.HivePushJoinThroughJoinRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.HiveSwapJoinRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.PropagateBucketTraitUpwardsRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.PropagateSortTraitUpwardsRule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelTraitSet;

public class CBO implements Frameworks.PlannerAction<RelNode> {
    private static final List<OperatorType> m_unsupportedOpTypes = Arrays
            .asList(OperatorType.DEMUX, OperatorType.FORWARD,
                    OperatorType.LATERALVIEWFORWARD,
                    OperatorType.LATERALVIEWJOIN, OperatorType.MUX,
                    OperatorType.PTF, OperatorType.SCRIPT, OperatorType.UDTF);

    @SuppressWarnings("rawtypes")
    private final Operator m_sinkOp;
    private final SemanticAnalyzer m_semanticAnalyzer;
    private final HiveConf m_conf;

    public CBO(@SuppressWarnings("rawtypes") Operator sinkOp,
            SemanticAnalyzer semanticAnalyzer, HiveConf conf) {
        m_sinkOp = sinkOp;
        m_semanticAnalyzer = semanticAnalyzer;
        m_conf = conf;
    }

    public static ASTNode optimize(
            @SuppressWarnings("rawtypes") Operator sinkOp,
            SemanticAnalyzer semanticAnalyzer, HiveConf conf) {
        ASTNode optiqOptimizedAST = null;

        if (shouldRunOptiqOptimizer(sinkOp, conf)) {
            RelNode optimizedOptiqPlan = Frameworks.withPlanner(new CBO(sinkOp,
                    semanticAnalyzer, conf));

            // return
            // OptiqOpToHiveASTConverter.convertOpTree(optimizedOptiqPlan);
            optiqOptimizedAST = OptiqRelToHiveASTConverter
                    .convertOpTree(optimizedOptiqPlan);
        }

        return optiqOptimizedAST;
    }

    @Override
    public RelNode apply(RelOptCluster cluster, RelOptSchema relOptSchema,
            Schema schema) {
        Long totalMemForSmallTable = m_conf
                .getLongVar(HiveConf.ConfVars.HIVESMALLTABLESFILESIZE);

        RelNode opTreeInOptiq = HiveToOptiqRelConverter.convertOpDAG(cluster,
                relOptSchema, schema, m_sinkOp, m_semanticAnalyzer, m_conf);

        // TODO: Add rules for projection pruning, predicate push down,
        // transitive predicate propagation
        cluster.getPlanner().addRule(new HiveSwapJoinRule());
        cluster.getPlanner().addRule(HivePushJoinThroughJoinRule.LEFT);
        cluster.getPlanner().addRule(HivePushJoinThroughJoinRule.RIGHT);
        cluster.getPlanner().addRule(new ConvertToCommonJoinRule());
        cluster.getPlanner().addRule(PropagateBucketTraitUpwardsRule.FILTER);
        cluster.getPlanner().addRule(PropagateBucketTraitUpwardsRule.LIMIT);
        cluster.getPlanner().addRule(PropagateBucketTraitUpwardsRule.PROJECT);
        cluster.getPlanner().addRule(PropagateSortTraitUpwardsRule.FILTER);
        cluster.getPlanner().addRule(PropagateSortTraitUpwardsRule.LIMIT);
        cluster.getPlanner().addRule(PropagateSortTraitUpwardsRule.PROJECT);
        cluster.getPlanner().addRule(
                new ConvertToBucketJoinRule(totalMemForSmallTable));
        cluster.getPlanner().addRule(new ConvertToSMBJoinRule());
        cluster.getPlanner().addRule(
                new ConvertToMapJoinRule(totalMemForSmallTable));

        RelTraitSet desiredTraits = opTreeInOptiq.getTraitSet().replace(
                HiveRel.CONVENTION);
        final RelNode rootRel = cluster.getPlanner().changeTraits(
                opTreeInOptiq, desiredTraits);
        cluster.getPlanner().setRoot(rootRel);

        return cluster.getPlanner().findBestExp();
    }

    private static boolean shouldRunOptiqOptimizer(Operator sinkOp,
            HiveConf conf) {
        boolean runOptiq = false;
        HashSet<Operator> start = new HashSet<Operator>();
        HashSet<OperatorType> opsThatsNotSupported = new HashSet<OperatorType>(
                m_unsupportedOpTypes);

        start.add(sinkOp);
        // TODO: use queryproperties instead of walking the tree
        if (!OperatorUtils.operatorExists(start, true, opsThatsNotSupported)) {
            runOptiq = true;
        }

        return runOptiq;
    }
}
