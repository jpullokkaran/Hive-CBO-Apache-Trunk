package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators2;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.optiq.schema.HiveSchema;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.eigenbase.rel.RelNode;

public interface HiveRelNode extends RelNode {

	/*
	 * return the original Hive Operator attached to this node.
	 */
	Operator<? extends OperatorDesc> attachedHiveOperator();
	
	/*
	 * on the best plan a Node maybe called to convert itself to a Hive Operator.
	 */
	Operator<? extends OperatorDesc> buildHiveOperator();
	
	HiveSchema getSchema();
	
}
