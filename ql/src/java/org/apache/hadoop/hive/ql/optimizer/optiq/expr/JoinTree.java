package org.apache.hadoop.hive.ql.optimizer.optiq.expr;

import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;



public class JoinTree {

	ImmutableSet<String> leftAliases;
	ImmutableSet<String> rightAliases;
	JoiningCondition cond;
	String outputAlias;
	
	static class JoiningCondition {
		JoiningExpression left;
		JoiningExpression right;
		
		JoiningCondition() {
		}
		
		JoiningCondition(JoiningExpression l, JoiningExpression r) {
			left = l;
			right = r;
		}
		
		JoiningCondition replaceAlias(String newAlias, boolean replaceLeft) 
				throws SemanticException {
			JoiningExpression l = left;
			JoiningExpression r = right;
			
			if(replaceLeft) {
				l = l.replaceAlias(newAlias);
			} else {
				r = r.replaceAlias(newAlias);
			}
			return new JoiningCondition(l, r);
		}
	}
	
	static class JoiningExpression {
		ExprNodeDesc expr;
		ImmutableSet<String> aliases;
		
		JoiningExpression() {}
		
		JoiningExpression(ExprNodeDesc expr, ImmutableSet<String> aliases) {
			super();
			this.expr = expr;
			this.aliases = aliases;
		}

		JoiningExpression replaceAlias(String newAlias) throws SemanticException {
			ExprNodeDesc e = ExprNodeUtils.mapReference(expr, newAlias);
			ImmutableSet.Builder<String> b = ImmutableSet.builder();
			return new JoiningExpression(e, b.add(newAlias).build());
		}
		
	}
	
	void makeSubQueryRight(String newAlias) throws SemanticException {
		cond = cond.replaceAlias(newAlias, false);
		ImmutableSet.Builder<String> b = ImmutableSet.builder();
		rightAliases = b.add(newAlias).build();
	}
	
	public ImmutableList<JoinTree> swap(JoinTree leftTree, JoinTree rightTree) throws SemanticException {
		ImmutableSet.Builder<String> b = ImmutableSet.builder();
		ImmutableSet<String> r = rightAliases;
		JoiningCondition  jc = cond;
		
		if ( leftTree != null && rightTree == null && leftTree.outputAlias == null ) {
			String newAlias = ExprNodeUtils.newTabAlias();
			leftTree.outputAlias = newAlias;
			jc = jc.replaceAlias(newAlias, true);
			r= b.add(newAlias).build();
		}
		
		JoinTree jT = new JoinTree();
		jT.leftAliases = r;
		jT.rightAliases = leftAliases;
		jT.cond = new JoiningCondition();
		jT.cond.left = jc.right;
		jT.cond.right = jc.left;
		
		ImmutableList.Builder<JoinTree> tB = ImmutableList.builder();
		return tB.add(jT).add(rightTree).add(leftTree).build();
	}
	
}
