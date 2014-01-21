package org.apache.hadoop.hive.ql.optimizer.optiq.expr;

import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


public class JoinTree {

	ImmutableSet<String> leftAliases;
	ImmutableSet<String> rightAliases;
	ImmutableList<JoiningCondition> conds;
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
		
		JoiningCondition swap() {
			return new JoiningCondition(new JoiningExpression(right), new JoiningExpression(left));
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
		
		JoiningExpression(JoiningExpression jE) {
			super();
			this.expr = jE.expr;
			this.aliases = ImmutableSet.copyOf(aliases);
		}

		JoiningExpression replaceAlias(String newAlias) throws SemanticException {
			ExprNodeDesc e = ExprNodeUtils.mapReference(expr, newAlias);
			ImmutableSet.Builder<String> b = ImmutableSet.builder();
			return new JoiningExpression(e, b.add(newAlias).build());
		}
		
	}
	
	JoinTree makeSubQueryRight(String newAlias) throws SemanticException {
		ImmutableList.Builder<JoiningCondition>  jcB = new ImmutableList.Builder<JoiningCondition>();
		for(JoiningCondition jC : conds) {
			jcB.add(jC.replaceAlias(newAlias, false));
		}
		JoinTree jT = new JoinTree();
		jT.conds = jcB.build();
		ImmutableSet.Builder<String> b = ImmutableSet.builder();
		jT.rightAliases = b.add(newAlias).build();
		jT.leftAliases = ImmutableSet.copyOf(leftAliases);
		jT.outputAlias = outputAlias;
		return jT;
	}
	
	public ImmutableList<JoinTree> swap(JoinTree leftTree, JoinTree rightTree) throws SemanticException {
		ImmutableSet.Builder<String> b = ImmutableSet.builder();
		ImmutableSet<String> r = rightAliases;
		ImmutableList.Builder<JoiningCondition>  jcB = new ImmutableList.Builder<JoiningCondition>();
		
		if ( leftTree != null && rightTree == null && leftTree.outputAlias == null ) {
			String newAlias = ExprNodeUtils.newTabAlias();
			leftTree.outputAlias = newAlias;
			for(JoiningCondition jC : conds) {
				jcB.add(jC.replaceAlias(newAlias, true));
			}
			r= b.add(newAlias).build();
		} else {
			for(JoiningCondition jC : conds) {
				jcB.add(jC.swap());
			}
		}
		
		JoinTree jT = new JoinTree();
		jT.leftAliases = r;
		jT.rightAliases = leftAliases;
		jT.conds = jcB.build();
		
		ImmutableList.Builder<JoinTree> tB = ImmutableList.builder();
		return tB.add(jT).add(rightTree).add(leftTree).build();
	}
	
}
