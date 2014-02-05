package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.optiq.expr.RexNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.schema.TypeConverter;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class RelNodeConverter {

	public static RelNode convert(Operator<? extends OperatorDesc> sinkOp, 
			RelOptCluster cluster, RelOptSchema schema,
			SemanticAnalyzer sA, ParseContext pCtx)  {

		Context ctx = new Context(cluster, schema, sA, pCtx);

		Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();
		rules.put(
				new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%"),
				new TableScanProcessor());
		rules.put(new RuleRegExp("R2", FilterOperator.getOperatorName() + "%"),
				new FilterProcessor());
		rules.put(new RuleRegExp("R3", SelectOperator.getOperatorName() + "%"),
				new SelectProcessor());
		rules.put(new RuleRegExp("R4", JoinOperator.getOperatorName() + "%"),
				new JoinProcessor());

		Dispatcher disp = new DefaultRuleDispatcher(new DefaultProcessor(),
				rules, ctx);
		GraphWalker egw = new ForwardWalker(disp);

		ArrayList<Node> topNodes = new ArrayList<Node>();
	    topNodes.addAll(pCtx.getTopOps().values());

		HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
		try {
			egw.startWalking(topNodes, outputMap);
		} catch (SemanticException se) {
			// @revisit
			throw new RuntimeException(se);
		}
		return (HiveRel) outputMap.get(sinkOp);
	}
	
	static class Context implements NodeProcessorCtx {
		RelOptCluster cluster;
		RelOptSchema schema;
		SemanticAnalyzer sA;
		ParseContext parseCtx;
		/*
		 * A Map from hive column internalNames to Optiq positions.
		 * A separate map for each Operator.
		 */
		Map<RelNode, ImmutableMap<String, Integer>> opPositionMap;
		
		Map<Operator<? extends OperatorDesc>, RelNode> hiveOpToRelNode;
		
		public Context(RelOptCluster cluster, RelOptSchema schema,
				SemanticAnalyzer sA,
				ParseContext parseCtx) {
			super();
			this.cluster = cluster;
			this.schema = schema;
			this.sA = sA;
			this.parseCtx = parseCtx;
			opPositionMap = new HashMap<RelNode, ImmutableMap<String,Integer>>();
			hiveOpToRelNode = new HashMap<Operator<? extends OperatorDesc>, RelNode>();
		}

		void buildColumnMap(Operator<? extends OperatorDesc> op, RelNode rNode) {
			RowSchema rr = op.getSchema();
			ImmutableMap.Builder<String, Integer> b = new ImmutableMap.Builder<String, Integer>();
			int i=0;
			for(ColumnInfo ci : rr.getSignature() ) {
				b.put(ci.getInternalName(), i);
				i++;
			}
			opPositionMap.put(rNode, b.build());
		}
		
		/*
		 * Why special handling for TableScan?
		 * - the RowResolver coming from hive for TScan still has all the columns,
		 *   whereas the Optiq type we build is based on the needed columns in the
		 *   TScanOp.
		 */
		void buildColumnMap(TableScanOperator tsOp, RelNode rNode) {
			RelDataType oType = rNode.getRowType();
			int i=0;
			ImmutableMap.Builder<String, Integer> b = new ImmutableMap.Builder<String, Integer>();
			for(String fN : oType.getFieldNames()) {
				b.put(fN, i);
				i++;
			}
			opPositionMap.put(rNode, b.build());
		}
		
		/*
		 * The Optiq JoinRel datatype is formed by combining the columns from its input RelNodes.
		 * Whereas the Hive RowResolver of the JoinOp contains only the columns needed by childOps.
		 */
		void buildColumnMap(JoinOperator jOp, HiveJoinRel jRel) throws SemanticException {			
			RowResolver rr = sA.getRowResolver(jOp);
			QBJoinTree hTree = parseCtx.getJoinContext().get(jOp);
			
			Map<String,Integer> leftMap = opPositionMap.get(jRel.getLeft());
			Map<String,Integer> rightMap = opPositionMap.get(jRel.getRight());
			int leftColCount = jRel.getLeft().getRowType().getFieldCount();
			ImmutableMap.Builder<String, Integer> b = new ImmutableMap.Builder<String, Integer>();
			for(Map.Entry<String, LinkedHashMap<String, ColumnInfo>> tableEntry : rr.getRslvMap().entrySet()) {
				String table = tableEntry.getKey();
				LinkedHashMap<String, ColumnInfo> cols = tableEntry.getValue();
				Map<String,Integer> posMap = leftMap;
				int offset = 0;
				if ( hTree.getRightAliases() != null ) {
					for(String rAlias : hTree.getRightAliases()) {
						if ( table.equals(rAlias)) {
							posMap = rightMap;
							offset = leftColCount;
							break;
						}
					}
				}
				for(Map.Entry<String, ColumnInfo> colEntry : cols.entrySet()) {
					ColumnInfo ci = colEntry.getValue();
					int pos = columnPosition(jOp, colEntry, posMap);
					b.put(ci.getInternalName(), pos+offset);
				}
			}
			opPositionMap.put(jRel, b.build());
		}
		
		/*
		 * map a JoinOp output column to its position in the JoinRelNode.
		 * This is surprisingly complicated by the following factor:
		 * - in the case of TableScan -> RedSink -> Join the JoinOp RR and the input PosMap 
		 *   use table column names. So we us the column alias from the RowResolver to find
		 *   the position.
		 * - In other cases: if the child is another Join or a SubQuery. 
		 *   The Input PosMap contains internal names generated by the Join or SelectOperator.
		 *   But the Join RR table-colAlias still reference the underlying table. To get to
		 *   the output columns names of the Input Operator  to the Join, we have to dereference
		 *   via the ColumnExprMap.
		 */
		private int columnPosition(JoinOperator jOp, 
				Map.Entry<String, ColumnInfo> colEntry, 
				Map<String, Integer> inpPosMap) {
			ColumnInfo ci = colEntry.getValue();
			ExprNodeDesc e = jOp.getColumnExprMap().get(ci.getInternalName());
			String cName = ((ExprNodeColumnDesc)e).getColumn();
			cName = cName.substring("VALUE.".length());
			int pos = -1;
			if ( inpPosMap.containsKey(cName)) {
				pos = inpPosMap.get(cName);
			} else {
				pos = inpPosMap.get(colEntry.getKey());
			}					
			return pos;
		}
		
		RexNode convertToOptiqExpr(final ExprNodeDesc expr,
				final RelNode optiqOP) {
			return convertToOptiqExpr(expr, optiqOP, 0);
		}

		RexNode convertToOptiqExpr(final ExprNodeDesc expr,
				final RelNode optiqOP, int offset) {
			ImmutableMap<String, Integer> posMap = opPositionMap.get(optiqOP);
			RexNodeConverter c = new RexNodeConverter(cluster,
					optiqOP.getRowType(), posMap, offset);
			return c.convert(expr);
		}
		
		RelNode getParentNode(Operator<? extends OperatorDesc> hiveOp, int i) {
			Operator<? extends OperatorDesc> p = hiveOp.getParentOperators().get(i);
			return p == null ? null :hiveOpToRelNode.get(p);
		}

	}
	
	static class JoinProcessor implements NodeProcessor {
		@SuppressWarnings("unchecked")
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			Context ctx = (Context) procCtx;
			HiveRel left = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 0);
			HiveRel right = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 1);
			JoinOperator joinOp = (JoinOperator) nd;
			JoinCondDesc[] jConds = joinOp.getConf().getConds();
			assert jConds.length == 1;
			HiveJoinRel joinRel = convertJoinOp(ctx, joinOp, jConds[0], left, right);
			ctx.buildColumnMap(joinOp, joinRel);
			ctx.hiveOpToRelNode.put(joinOp, joinRel);
			return joinRel;
		}

		/*
		 * @todo: cleanup, for now just copied from HiveToOptiqRelConvereter
		 */
		private HiveJoinRel convertJoinOp(Context ctx, JoinOperator op,
				JoinCondDesc jc, HiveRel leftRel, HiveRel rightRel) {
			HiveJoinRel joinRel = null;
			Operator<? extends OperatorDesc> leftParent = op
					.getParentOperators().get(jc.getLeft());
			Operator<? extends OperatorDesc> rightParent = op
					.getParentOperators().get(jc.getRight());

			if (leftParent instanceof ReduceSinkOperator
					&& rightParent instanceof ReduceSinkOperator) {
				List<ExprNodeDesc> leftCols = ((ReduceSinkDesc) (leftParent
						.getConf())).getKeyCols();
				List<ExprNodeDesc> rightCols = ((ReduceSinkDesc) (rightParent
						.getConf())).getKeyCols();
				RexNode joinPredicate = null;
				JoinRelType joinType = JoinRelType.INNER;
				int rightColOffSet = leftRel.getRowType().getFieldCount();

				// TODO: what about semi join
				switch (jc.getType()) {
				case JoinDesc.INNER_JOIN:
					joinType = JoinRelType.INNER;
					break;
				case JoinDesc.LEFT_OUTER_JOIN:
					joinType = JoinRelType.LEFT;
					break;
				case JoinDesc.RIGHT_OUTER_JOIN:
					joinType = JoinRelType.RIGHT;
					break;
				case JoinDesc.FULL_OUTER_JOIN:
					joinType = JoinRelType.FULL;
					break;
				}

				int i = 0;
				for (ExprNodeDesc expr : leftCols) {
					List<RexNode> eqExpr = new LinkedList<RexNode>();
					eqExpr.add(ctx.convertToOptiqExpr(expr, leftRel, 0));
					eqExpr.add(ctx.convertToOptiqExpr(rightCols.get(i), rightRel,
							rightColOffSet));

					RexNode eqOp = ctx.cluster.getRexBuilder().makeCall(
							SqlStdOperatorTable.EQUALS, eqExpr);
					i++;

					if (joinPredicate == null) {
						joinPredicate = eqOp;
					} else {
						List<RexNode> conjElements = new LinkedList<RexNode>();
						conjElements.add(joinPredicate);
						conjElements.add(eqOp);
						joinPredicate = ctx.cluster.getRexBuilder().makeCall(
								SqlStdOperatorTable.AND, conjElements);
					}
				}

				// Translate non-joinkey predicate
				Set<Entry<Byte, List<ExprNodeDesc>>> filterExprSet = op
						.getConf().getFilters().entrySet();
				if (filterExprSet != null && !filterExprSet.isEmpty()) {
					RexNode eqExpr;
					int colOffSet = 0;
					RelNode childRel;
					Operator parentHiveOp;
					int inputId;

					for (Entry<Byte, List<ExprNodeDesc>> entry : filterExprSet) {
						inputId = entry.getKey().intValue();
						if (inputId == 0) {
							colOffSet = 0;
							childRel = leftRel;
							parentHiveOp = leftParent;
						} else if (inputId == 1) {
							colOffSet = rightColOffSet;
							childRel = rightRel;
							parentHiveOp = rightParent;
						} else {
							throw new RuntimeException("Invalid Join Input");
						}

						for (ExprNodeDesc expr : entry.getValue()) {
							eqExpr = ctx.convertToOptiqExpr(expr,
									childRel, colOffSet);
							List<RexNode> conjElements = new LinkedList<RexNode>();
							conjElements.add(joinPredicate);
							conjElements.add(eqExpr);
							joinPredicate = ctx.cluster.getRexBuilder()
									.makeCall(SqlStdOperatorTable.AND,
											conjElements);
						}
					}
				}

				joinRel = HiveJoinRel.getJoin(ctx.cluster, leftRel, rightRel,
						joinPredicate, joinType);
			} else {
				throw new RuntimeException(
						"Right & Left of Join Condition columns are not equal");
			}
			
			return joinRel;
		}
	}
	
	static class FilterProcessor implements NodeProcessor {
		@SuppressWarnings("unchecked")
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			Context ctx = (Context) procCtx;
			HiveRel input = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 0);
			FilterOperator filterOp = (FilterOperator) nd;
			RexNode convertedFilterExpr = ctx.convertToOptiqExpr(filterOp
					.getConf().getPredicate(), input);

			HiveRel filtRel = new HiveFilterRel(ctx.cluster,
					ctx.cluster.traitSetOf(HiveRel.CONVENTION), input,
					convertedFilterExpr);
			ctx.buildColumnMap(filterOp, filtRel);
			ctx.hiveOpToRelNode.put(filterOp, filtRel);
			return filtRel;
		}
	}

	static class SelectProcessor implements NodeProcessor {
		@SuppressWarnings("unchecked")
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			Context ctx = (Context) procCtx;
			HiveRel inputRelNode = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 0);
			SelectOperator selectOp = (SelectOperator) nd;

			List<ExprNodeDesc> colLst = selectOp.getConf().getColList();
			List<RexNode> optiqColLst = new LinkedList<RexNode>();

			for (ExprNodeDesc colExpr : colLst) {
				optiqColLst.add(ctx.convertToOptiqExpr(colExpr,
						inputRelNode));
			}
			
			/*
			 * Hive treats names that start with '_c' as internalNames; so change the names so we
			 * don't run into this issue when converting back to Hive AST.
			 */
			List<String> oFieldNames = Lists.transform(selectOp.getConf().getOutputColumnNames(),
					new Function<String, String>() {
						public String apply( String hName ){
				            return "_o_" + hName;
				         }
			});

			HiveRel selRel = new HiveProjectRel(ctx.cluster, inputRelNode,
					optiqColLst, oFieldNames,
					ProjectRel.Flags.BOXED);
			ctx.buildColumnMap(selectOp, selRel);
			ctx.hiveOpToRelNode.put(selectOp, selRel);
			return selRel;
		}
	}

	static class TableScanProcessor implements NodeProcessor {
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			Context ctx = (Context) procCtx;
			TableScanOperator tableScanOp = (TableScanOperator) nd;
			RowResolver rr = ctx.sA.getRowResolver(tableScanOp);

			List<String> neededCols = tableScanOp.getNeededColumns();
			RelDataType rowType = TypeConverter.getType(ctx.cluster, rr,
					neededCols);
			RelOptHiveTable optTable = new RelOptHiveTable(ctx.schema,
					tableScanOp.getConf().getAlias(), rowType,
					ctx.sA.getTable(tableScanOp), tableScanOp.getSchema(),
					ctx.parseCtx.getConf());
			TableAccessRelBase tableRel = new HiveTableScanRel(ctx.cluster,
					ctx.cluster.traitSetOf(HiveRel.CONVENTION), optTable,
					rowType);
			ctx.buildColumnMap(tableScanOp, tableRel);
			ctx.hiveOpToRelNode.put(tableScanOp, tableRel);
			return tableRel;
		}
	}

	static class DefaultProcessor implements NodeProcessor {
		public Object process(Node nd, Stack<Node> stack,
				NodeProcessorCtx procCtx, Object... nodeOutputs)
				throws SemanticException {
			@SuppressWarnings("unchecked")
			Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
			Context ctx = (Context) procCtx;
			RelNode node = (HiveRel) ctx.getParentNode(op, 0);
			ctx.hiveOpToRelNode.put(op, node);
			return node;
		}
	}
}
