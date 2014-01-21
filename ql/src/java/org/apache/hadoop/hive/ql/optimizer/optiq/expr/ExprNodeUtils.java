package org.apache.hadoop.hive.ql.optimizer.optiq.expr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class ExprNodeUtils {
	
	private static AtomicInteger tabAliasCount = new AtomicInteger(0);
	
	public static String newTabAlias() {
		return String.format("_t%d", tabAliasCount.incrementAndGet());
	}
	
	public static ExprNodeDesc mapReference(ExprNodeDesc inp,
			ImmutableMap<String, String> aliasMap) throws SemanticException {

		AliasMapper.AliasMapping exprCtx = new AliasMapper.ExplicitAliasMapping(aliasMap);
		return mapReference(inp, exprCtx);
	}
	
	public static ExprNodeDesc mapReference(ExprNodeDesc inp, String newAlias) throws SemanticException {

		AliasMapper.AliasMapping exprCtx = new AliasMapper.AllTablesAliasMapping(newAlias);
		return mapReference(inp, exprCtx);
	}
	
	public static ImmutableSet<String> findAliases(ExprNodeDesc inp, RowResolver rr)  
	throws SemanticException {
		
		AliasCollectorCtx ctx = new AliasCollectorCtx(rr);
		Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
		exprRules.put(new RuleRegExp("R1", ExprNodeColumnDesc.class.getName() + "%"), ctx);

		Dispatcher disp = new DefaultRuleDispatcher(ctx, exprRules, ctx);
		GraphWalker egw = new DefaultGraphWalker(disp);

		List<Node> startNodes = new ArrayList<Node>();
		startNodes.add(inp);

		HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
		egw.startWalking(startNodes, outputMap);
		return ctx.b.build();
	}
	
	protected static ExprNodeDesc mapReference(ExprNodeDesc inp,
			AliasMapper.AliasMapping exprCtx) throws SemanticException {

		Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
		exprRules.put(new RuleRegExp("R1", ExprNodeColumnDesc.class.getName() + "%"), new AliasMapper.ColumnExprProcessor());

		Dispatcher disp = new DefaultRuleDispatcher(new AliasMapper.DefaultProcessor(), exprRules, exprCtx);
		GraphWalker egw = new DefaultGraphWalker(disp);

		List<Node> startNodes = new ArrayList<Node>();
		startNodes.add(inp);

		HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
		egw.startWalking(startNodes, outputMap);
		return (ExprNodeDesc) outputMap.get(inp);
	}


	static class AliasMapper {
		
		static interface AliasMapping extends NodeProcessorCtx {
			public String replace(String iAlias);
		}

		static class ExplicitAliasMapping implements AliasMapping {
			ImmutableMap<String, String> aliasMap;

			public ExplicitAliasMapping(ImmutableMap<String, String> aliasMap) {
				super();
				this.aliasMap = aliasMap;
			}
			
			public String replace(String iAlias) {
				String oAlias = iAlias == null ? null : aliasMap.get(iAlias);
				return oAlias;
			}

		};
		
		static class AllTablesAliasMapping implements AliasMapping {
			String outAlias;

			public AllTablesAliasMapping(String outAlias) {
				this.outAlias = outAlias;
			}
			
			public String replace(String iAlias) {
				return outAlias;
			}

		};

		static class DefaultProcessor implements NodeProcessor {

			public Object process(Node nd, Stack<Node> stack,
					NodeProcessorCtx procCtx, Object... nodeOutputs)
					throws SemanticException {

				ExprNodeDesc e = (ExprNodeDesc) nd;
				List<ExprNodeDesc> children = e.getChildren();
				if (children != null) {
					for (int i = 0; i < children.size(); i++) {
						children.set(i, (ExprNodeDesc) nodeOutputs[i]);
					}
				}

				return nd;

			}
		}

		static class ColumnExprProcessor implements NodeProcessor {

			public Object process(Node nd, Stack<Node> stack,
					NodeProcessorCtx procCtx, Object... nodeOutputs)
					throws SemanticException {

				AliasMapping m = (AliasMapping) procCtx;
				ExprNodeColumnDesc c = (ExprNodeColumnDesc) nd;
				String oAlias = m.replace(c.getTabAlias());

				if (oAlias != null) {
					boolean skew = c.isSkewedCol();
					c = new ExprNodeColumnDesc(c.getTypeInfo(), c.getColumn(),
							oAlias,
							c.getIsPartitionColOrVirtualCol());
					c.setSkewedCol(skew);
				}
				return c;
			}
		}
	};
	
	static class AliasCollectorCtx implements NodeProcessorCtx, NodeProcessor {
		ImmutableSet.Builder<String> b = new ImmutableSet.Builder<String>();
		RowResolver rr;
		
		AliasCollectorCtx(RowResolver rr) {
			this.rr = rr;
		}
		
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			if ( nd instanceof ExprNodeColumnDesc ) {
				ExprNodeColumnDesc c = (ExprNodeColumnDesc) nd;
				AliasCollectorCtx ctx = (AliasCollectorCtx) procCtx;
				String[] alias = rr.getInvRslvMap().get(c.getName());
				b.add(alias[0]);
			}
			return nd;
		}
	}

}
