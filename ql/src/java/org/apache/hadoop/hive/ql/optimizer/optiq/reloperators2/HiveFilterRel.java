package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators2;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCosts;
import org.apache.hadoop.hive.ql.optimizer.optiq.expr.RexNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.optiq.schema.HiveSchema;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

public class HiveFilterRel extends FilterRelBase implements HiveRelNode {
	
	public static HiveFilterRel create(RelOptCluster cluster, 
			RelOptSchema schema, 
			RelTraitSet traits,
			FilterOperator filterOp, 
			RowResolver rr,
			HiveRelNode input) {
		
		HiveSchema ihSch = input.getSchema();
		RexNodeConverter rC = new RexNodeConverter(cluster, ihSch);
		RexNode oExpr = rC.convert(filterOp.getConf().getPredicate());
		HiveSchema hSch = HiveSchema.createSchema(cluster, rr);

	    return new HiveFilterRel(filterOp, hSch,
	    		cluster, 
	    		traits,
	    		input, 
	    		oExpr);
		
	}

	HiveSchema hSchema;
	FilterOperator filterOp;
	Statistics hiveStats;
	
	protected HiveFilterRel(FilterOperator filterOp,
			HiveSchema hSch,
			RelOptCluster cluster, 
			RelTraitSet traits,
			RelNode child, 
			RexNode condition) {
		super(cluster, traits, child, condition);
		hSchema = hSch;
		this.filterOp = filterOp;
		checkNotNull(filterOp.getStatistics());
		hiveStats = filterOp.getStatistics();
	}

	@Override
	public double getRows() {
		return hiveStats.getNumRows();
	}

	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner) {
		double dRows = RelMetadataQuery.getRowCount(getChild());
        double dCpu = dRows * HiveCosts.PROCESSING_PER_UNIT ;
        double dIo = 0;
        return planner.makeCost(getRows(), dCpu, dIo);
    }
	
	@Override
	protected RelDataType deriveRowType() {
        return hSchema.getRelDataType();
    }

	@Override
	public Operator<? extends OperatorDesc> attachedHiveOperator() {
		return filterOp;
	}

	@Override
	public Operator<? extends OperatorDesc> buildHiveOperator() {
		if (filterOp != null ) {
			return filterOp;
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public HiveSchema getSchema() {
		return hSchema;
	}
}
