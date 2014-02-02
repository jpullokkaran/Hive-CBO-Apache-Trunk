package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.Frameworks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveVolcanoPlanner;
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
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.CachingRelMetadataProvider;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptQuery;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.sql.SqlExplainLevel;

public class CBO implements Frameworks.PlannerAction<RelNode> {
    private static final List<OperatorType> m_unsupportedOpTypes = Arrays
            .asList(OperatorType.DEMUX, OperatorType.FORWARD,
                    OperatorType.LATERALVIEWFORWARD,
                    OperatorType.LATERALVIEWJOIN, OperatorType.MUX,
                    OperatorType.PTF, OperatorType.SCRIPT, OperatorType.UDTF, OperatorType.GROUPBY, OperatorType.LIMIT, OperatorType.UNION);

    @SuppressWarnings("rawtypes")
    private final Operator m_sinkOp;
    private final SemanticAnalyzer m_semanticAnalyzer;
    private final HiveConf m_conf;
    private final ParseContext m_ParseContext;

    public CBO(@SuppressWarnings("rawtypes") Operator sinkOp,
            SemanticAnalyzer semanticAnalyzer, ParseContext pCtx) {
        m_sinkOp = sinkOp;
        m_semanticAnalyzer = semanticAnalyzer;
        m_ParseContext = pCtx;
        m_conf = pCtx.getConf();
    }

    public static ASTNode optimize(
            @SuppressWarnings("rawtypes") Operator sinkOp,
            SemanticAnalyzer semanticAnalyzer, ParseContext pCtx) {
        ASTNode optiqOptimizedAST = null;
        HiveConf conf = pCtx.getConf();
        if (shouldRunOptiqOptimizer(sinkOp, conf, semanticAnalyzer.getQueryProperties())) {
            RelNode optimizedOptiqPlan = Frameworks.withPlanner(new CBO(sinkOp,
                    semanticAnalyzer, pCtx));

            // return
            // OptiqOpToHiveASTConverter.convertOpTree(optimizedOptiqPlan);
            optiqOptimizedAST = OptiqRelToHiveASTConverter
                    .convertOpTree(optimizedOptiqPlan);
        }

        return optiqOptimizedAST;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RelNode apply(RelOptCluster cluster, RelOptSchema relOptSchema,
            SchemaPlus schema) {
        Long totalMemForSmallTable = m_conf
                .getLongVar(HiveConf.ConfVars.HIVESMALLTABLESFILESIZE);

      //RelOptPlanner planner = cluster.getPlanner();
        RelOptPlanner planner = HiveVolcanoPlanner.createPlanner();
        //planner.addRelTraitDef(RelBucketingTraitDef.INSTANCE);
        
        /*
         * recreate cluster, so that it picks up the additional traitDef
         */
        RelOptQuery query = new RelOptQuery(planner);
        RexBuilder rexBuilder = cluster.getRexBuilder();
        cluster =
            query.createCluster(rexBuilder.getTypeFactory(), rexBuilder);
        
        
        /*
         * wrap MetaDataProvider in a Caching Provider.
         */
        cluster.setMetadataProvider(
        		new CachingRelMetadataProvider(cluster.getMetadataProvider(), planner));

        
        RelNode opTreeInOptiq =  RelNodeConverter.convert(m_sinkOp,
    			cluster, relOptSchema,
    			m_semanticAnalyzer, m_ParseContext);
        
        /*
         * The starting tree
         */
        System.out.println(RelOptUtil.toString(opTreeInOptiq, SqlExplainLevel.ALL_ATTRIBUTES));

        cluster.getPlanner().clearRules();
        cluster.getPlanner().addRule(new HiveSwapJoinRule());
        cluster.getPlanner().addRule(HivePushJoinThroughJoinRule.LEFT);
        cluster.getPlanner().addRule(HivePushJoinThroughJoinRule.RIGHT);

        /*
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
        
        RelTraitSet desiredTraits = 
        		RelTraitSet.createEmpty().
        		plus(HiveRel.CONVENTION).
        		plus(RelCollationTraitDef.INSTANCE.getDefault()).
        		plus(RelBucketingTraitImpl.EMPTY);
        		*/
		RelTraitSet desiredTraits = RelTraitSet.createEmpty()
				.plus(HiveRel.CONVENTION)
				.plus(RelCollationTraitDef.INSTANCE.getDefault());

        final RelNode rootRel = cluster.getPlanner().changeTraits(
                opTreeInOptiq, desiredTraits);
        cluster.getPlanner().setRoot(rootRel);

        return cluster.getPlanner().findBestExp();
    }

	private static boolean shouldRunOptiqOptimizer(Operator sinkOp,
			HiveConf conf, QueryProperties qp) {
		boolean runOptiq = false;

		if ((qp.getJoinCount() < HiveConf.getIntVar(conf,
						HiveConf.ConfVars.HIVE_CBO_MAX_JOINS_SUPPORTED))
				&& (qp.getOuterJoinCount() == 0) && !qp.hasClusterBy()
				&& !qp.hasDistributeBy() && !qp.hasOrderBy() && !qp.hasSortBy()
				&& !qp.hasWindowing()) {
			final HashSet<Operator> start = new HashSet<Operator>();
			final HashSet<OperatorType> opsThatsNotSupported = new HashSet<OperatorType>(
					m_unsupportedOpTypes);

			start.add(sinkOp);
			// TODO: use queryproperties instead of walking the tree
			if (!OperatorUtils
					.operatorExists(start, true, opsThatsNotSupported)) {
				runOptiq = true;
			}
		}

		return runOptiq;
	}
}
