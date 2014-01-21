package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.optiq.expr.RexNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators2.HiveTableScanRel.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.optiq.schema.HiveSchema;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

public class HiveRelNodeBuilder {
	
	protected static HiveRelNode build(Operator<? extends OperatorDesc> sinkOp,
			RelOptCluster cluster, RelOptSchema schema, RelTraitSet traits,
			SemanticAnalyzer sA) throws SemanticException {
		
		BuilderCtx ctx = new BuilderCtx(cluster, schema, traits, sA);

		Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();
		rules.put(new RuleRegExp("R1", TableScanOperator.class.getName() + "%"), new TableScanProcessor());
		rules.put(new RuleRegExp("R2", FilterOperator.class.getName() + "%"), new FilterProcessor());
		rules.put(new RuleRegExp("R3", SelectOperator.class.getName() + "%"), new SelectProcessor());
		rules.put(new RuleRegExp("R4", JoinOperator.class.getName() + "%"), new JoinProcessor());

		Dispatcher disp = new DefaultRuleDispatcher(new DefaultProcessor(), rules, ctx);
		GraphWalker egw = new DefaultGraphWalker(disp);

		List<Node> startNodes = new ArrayList<Node>();
		startNodes.add(sinkOp);

		HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
		egw.startWalking(startNodes, outputMap);
		return (HiveRelNode) outputMap.get(sinkOp);
	}

	static class BuilderCtx implements NodeProcessorCtx {
		RelOptCluster cluster;
		RelOptSchema schema;
		RelTraitSet traits;
		SemanticAnalyzer sA;
		public BuilderCtx(RelOptCluster cluster, RelOptSchema schema,
				RelTraitSet traits, SemanticAnalyzer sA) {
			super();
			this.cluster = cluster;
			this.schema = schema;
			this.traits = traits;
			this.sA = sA;
		}
		
		
	}

	static class TableScanProcessor implements NodeProcessor {
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			BuilderCtx ctx = (BuilderCtx) procCtx;
			TableScanOperator tableScanOp = (TableScanOperator) nd;
			RowResolver rr = ctx.sA.getRowResolver(tableScanOp);
			HiveSchema hSch = HiveSchema.createSchema(ctx.cluster, rr);
			String name = tableScanOp.getConf().getAlias();
			RelDataType dT = hSch.getRelDataType();
			RelOptHiveTable oTbl = new RelOptHiveTable(tableScanOp, hSch,
					ctx.schema, name, dT);
			return new HiveTableScanRel(ctx.cluster, ctx.traits, oTbl);
		}
	}
	
	static class FilterProcessor implements NodeProcessor {
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			BuilderCtx ctx = (BuilderCtx) procCtx;
			HiveRelNode input = (HiveRelNode) nodeOutputs[0];
			FilterOperator filterOp = (FilterOperator) nd;
			RowResolver rr = ctx.sA.getRowResolver(filterOp);
			HiveSchema ihSch = input.getSchema();
			RexNodeConverter rC = new RexNodeConverter(ctx.cluster, ihSch);
			RexNode oExpr = rC.convert(filterOp.getConf().getPredicate());
			HiveSchema hSch = HiveSchema.createSchema(ctx.cluster, rr);

		    return new HiveFilterRel(filterOp, hSch,
		    		ctx.cluster, 
		    		ctx.traits,
		    		input, 
		    		oExpr);
		}
	}
	
	static class SelectProcessor implements NodeProcessor {
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			BuilderCtx ctx = (BuilderCtx) procCtx;
			HiveRelNode input = (HiveRelNode) nodeOutputs[0];
			SelectOperator selectOp = (SelectOperator) nd;
			RowResolver rr = ctx.sA.getRowResolver(selectOp);
			
			HiveSchema ihSch = input.getSchema();
			RexNodeConverter rC = new RexNodeConverter(ctx.cluster, ihSch);
		    List<RexNode> oExprs = new LinkedList<RexNode>();
		    
			for (ExprNodeDesc colExpr : selectOp.getConf().getColList()) {
				oExprs.add(rC.convert(colExpr));
			}

			HiveSchema hSch = HiveSchema.createSchema(ctx.cluster, rr);
			RelDataType dT = hSch.getRelDataType();

		    return new HiveProjectRel(selectOp, hSch,
		    		ctx.cluster, 
		    		ctx.traits,
		    		input, 
		    		oExprs, 
		    		dT,
		    		0);
			
		}
	}
	
	static class JoinProcessor implements NodeProcessor {
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			BuilderCtx ctx = (BuilderCtx) procCtx;
			HiveRelNode left = (HiveRelNode) nodeOutputs[0];
			HiveRelNode right = (HiveRelNode) nodeOutputs[1];
			JoinOperator joinOp = (JoinOperator) nd;
			RowResolver rr = ctx.sA.getRowResolver(joinOp);
			
			return null;
		}
	}
	
	static class DefaultProcessor implements NodeProcessor {
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			return nodeOutputs[0];
		}
	}
}
