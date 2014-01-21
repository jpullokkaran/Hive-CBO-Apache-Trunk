package org.apache.hadoop.hive.ql.optimizer.optiq.expr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.optimizer.optiq.expr.JoinTree.JoiningCondition;
import org.apache.hadoop.hive.ql.optimizer.optiq.expr.JoinTree.JoiningExpression;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators2.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators2.HiveRelNode;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.eigenbase.rel.RelNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class JoinTreeUtils {
	
	public static JoinTree build(SemanticAnalyzer sA,
			ReduceSinkOperator left, ReduceSinkOperator right, QBJoinTree qbJT ) 
	throws SemanticException {
		JoinTree jT = new JoinTree();
		jT.leftAliases = ImmutableSet.copyOf(qbJT.getLeftAliases());
		jT.rightAliases = ImmutableSet.copyOf(qbJT.getRightAliases());
		ReduceSinkDesc lDesc = left.getConf();
		ReduceSinkDesc rDesc = right.getConf();
		int numJExprs = lDesc.getKeyCols().size();
		RowResolver lRR = sA.getRowResolver(left.getParentOperators().get(0));
		RowResolver rRR = sA.getRowResolver(right.getParentOperators().get(0));
		
		ImmutableList.Builder<JoiningCondition>  jcB = new ImmutableList.Builder<JoiningCondition>();
		for(int i=0; i<numJExprs; i++) {
			jcB.add(joinCondition(lDesc.getKeyCols().get(i), lRR, rDesc.getKeyCols().get(i), rRR));
		}
		
		jT.conds = jcB.build();
		return jT;
	}	
	
	static JoiningCondition joinCondition(ExprNodeDesc lExpr, RowResolver lRR,
			ExprNodeDesc rExpr, RowResolver rRR) throws SemanticException {
		JoiningExpression lJE = new JoiningExpression(lExpr, ExprNodeUtils.findAliases(lExpr, lRR));
		JoiningExpression rJE = new JoiningExpression(rExpr, ExprNodeUtils.findAliases(rExpr, rRR));
		JoiningCondition jC = new JoiningCondition(lJE, rJE);
		return jC;
	}
	
	static JoinOperator buildJoinOperator(HiveJoinRel joinNode) {
		JoinTree jT = joinNode.getJoinTree();
		List<Operator<? extends OperatorDesc>> childOps = new ArrayList<Operator<? extends OperatorDesc>>();
		for(RelNode r : joinNode.getInputs()) {
			childOps.add(((HiveRelNode)(r.getInput(0))).buildHiveOperator());
		}
		return null;
	}
	
	
	static ReduceSinkOperator joinInput(SemanticAnalyzer sA, 
			ParseContext pCtx,
			Operator<? extends OperatorDesc> inpOp, String alias, JoinTree jT,
			boolean left) throws SemanticException {
		RowResolver inputRS = pCtx.getOpParseCtx().get(inpOp).getRowResolver();
		RowResolver outputRS = new RowResolver();
		ArrayList<String> outputColumns = new ArrayList<String>();
		ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();

		for(JoiningCondition jC : jT.conds) {
			JoiningExpression jE = left ? jC.left : jC.right;
			reduceKeys.add(jE.expr);
		}

		// Walk over the input row resolver and copy in the output
		ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
		Iterator<String> tblNamesIter = inputRS.getTableNames().iterator();
		Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
		while (tblNamesIter.hasNext()) {
			String src = tblNamesIter.next();
			HashMap<String, ColumnInfo> fMap = inputRS.getFieldMap(src);
			for (Map.Entry<String, ColumnInfo> entry : fMap.entrySet()) {
				String field = entry.getKey();
				ColumnInfo valueInfo = entry.getValue();
				ExprNodeColumnDesc inputExpr = new ExprNodeColumnDesc(
						valueInfo.getType(), valueInfo.getInternalName(),
						valueInfo.getTabAlias(), valueInfo.getIsVirtualCol());
				reduceValues.add(inputExpr);
				if (outputRS.get(src, field) == null) {
					String col = SemanticAnalyzer.getColumnInternalName(reduceValues.size() - 1);
					outputColumns.add(col);
					ColumnInfo newColInfo = new ColumnInfo(
							Utilities.ReduceField.VALUE.toString() + "." + col,
							valueInfo.getType(), src,
							valueInfo.getIsVirtualCol(),
							valueInfo.isHiddenVirtualCol());

					colExprMap.put(newColInfo.getInternalName(), inputExpr);
					outputRS.put(src, field, newColInfo);
				}
			}
		}

		int numReds = -1;

		// Use only 1 reducer in case of cartesian product
		if (reduceKeys.size() == 0) {
			numReds = 1;

			// Cartesian product is not supported in strict mode
			if (pCtx.getConf().getVar(HiveConf.ConfVars.HIVEMAPREDMODE)
					.equalsIgnoreCase("strict")) {
				throw new SemanticException(
						ErrorMsg.NO_CARTESIAN_PRODUCT.getMsg());
			}
		}

		ReduceSinkOperator rsOp = (ReduceSinkOperator) sA.putOpInsertMap(
				OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(
						reduceKeys, reduceValues, outputColumns, false,
						left ? 0 : 1, reduceKeys.size(), numReds),
						new RowSchema(outputRS.getColumnInfos()), inpOp),
				outputRS);
		rsOp.setColumnExprMap(colExprMap);
		rsOp.setInputAlias(alias);
		return rsOp;

	}
}
