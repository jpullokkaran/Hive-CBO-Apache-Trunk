package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators2;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCosts;
import org.apache.hadoop.hive.ql.optimizer.optiq.expr.RexNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.optiq.schema.HiveSchema;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

public class HiveProjectRel extends ProjectRelBase implements HiveRelNode {

	public static HiveProjectRel create(RelOptCluster cluster, 
			RelOptSchema schema, 
			RelTraitSet traits,
			SelectOperator selectOp, 
			RowResolver rr,
			HiveRelNode input) {
		
		HiveSchema ihSch = input.getSchema();
		RexNodeConverter rC = new RexNodeConverter(cluster, ihSch);
	    List<RexNode> oExprs = new LinkedList<RexNode>();
	    
		for (ExprNodeDesc colExpr : selectOp.getConf().getColList()) {
			oExprs.add(rC.convert(colExpr));
		}

		HiveSchema hSch = HiveSchema.createSchema(cluster, rr);
		RelDataType dT = hSch.getRelDataType();

	    return new HiveProjectRel(selectOp, hSch,
	    		cluster, 
	    		traits,
	    		input, 
	    		oExprs, 
	    		dT,
	    		0);
		
	}
	
	HiveSchema hSchema;
	SelectOperator selectOp;
	Statistics hiveStats;

	protected HiveProjectRel(SelectOperator selectOp,
			HiveSchema hSch,
			RelOptCluster cluster, 
			RelTraitSet traits,
			HiveRelNode child, 
			List<RexNode> exps, 
			RelDataType rowType, 
			int flags) {
		super(cluster, traits, child, exps, rowType, flags);
		hSchema = hSch;
		this.selectOp = selectOp;
		checkNotNull(selectOp.getStatistics());
		hiveStats = selectOp.getStatistics();
	}

	
	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner) {
		double dRows = RelMetadataQuery.getRowCount(getChild());
        double dCpu = dRows * HiveCosts.PROCESSING_PER_UNIT ;
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

	@Override
	public Operator<? extends OperatorDesc> attachedHiveOperator() {
		return selectOp;
	}

	@Override
	public Operator<? extends OperatorDesc> buildHiveOperator() {
		if (selectOp != null ) {
			return selectOp;
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public HiveSchema getSchema() {
		return hSchema;
	}
}
