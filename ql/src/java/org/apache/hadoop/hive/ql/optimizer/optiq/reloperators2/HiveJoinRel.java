package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators2;

import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.optimizer.optiq.expr.JoinTree;
import org.apache.hadoop.hive.ql.optimizer.optiq.schema.HiveSchema;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class HiveJoinRel extends JoinRelBase implements HiveRelNode {
	
	JoinTree joinTree;
	HiveSchema hSchema;
	SelectOperator joinOp;
	Statistics hiveStats;

	protected HiveJoinRel(RelOptCluster cluster, RelTraitSet traits,
			RelNode left, RelNode right, RexNode condition,
			JoinRelType joinType, Set<String> variablesStopped) {
		super(cluster, traits, left, right, condition, joinType, variablesStopped);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Operator<? extends OperatorDesc> attachedHiveOperator() {
		return joinOp;
	}

	@Override
	public Operator<? extends OperatorDesc> buildHiveOperator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HiveSchema getSchema() {
		return hSchema;
	}

	@Override
	public JoinRelBase copy(RelTraitSet traitSet, RexNode conditionExpr,
			RelNode left, RelNode right) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public JoinTree getJoinTree() {
		return joinTree;
	}

}
