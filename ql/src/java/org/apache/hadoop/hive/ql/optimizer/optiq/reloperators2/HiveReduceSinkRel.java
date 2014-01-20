package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators2;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCosts;
import org.apache.hadoop.hive.ql.optimizer.optiq.schema.HiveSchema;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;

public class HiveReduceSinkRel extends SingleRel implements HiveRelNode {
	
	public static HiveReduceSinkRel create(RelOptCluster cluster, 
			RelOptSchema schema, 
			RelTraitSet traits,
			ReduceSinkOperator rsOp, 
			RowResolver rr,
			HiveRelNode input) {
		
		HiveSchema hSch = HiveSchema.createSchema(cluster, rr);

	    return new HiveReduceSinkRel(rsOp, hSch,
	    		cluster, 
	    		traits,
	    		input);	
	}
	
	HiveSchema hSchema;
	ReduceSinkOperator rsOp;
	Statistics hiveStats;

	protected HiveReduceSinkRel(ReduceSinkOperator rsOp,
			HiveSchema hSchema,
			RelOptCluster cluster, RelTraitSet traits,
			RelNode child) {
		super(cluster, traits, child);
		this.rsOp = rsOp;
		this.hSchema = hSchema;
		checkNotNull(rsOp.getStatistics());
		hiveStats = rsOp.getStatistics();
	}

	@Override
	public double getRows() {
		return hiveStats.getNumRows();
	}

	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner) {
		double dRows = RelMetadataQuery.getRowCount(getChild());
        double dCpu = dRows * Math.log(dRows) * HiveCosts.PROCESSING_PER_UNIT ;
        double dIo = dRows * (HiveCosts.LOCAL_WRITE_PER_UNIT + HiveCosts.HDFS_READ_PER_UNIT );
        return planner.makeCost(getRows(), dCpu, dIo);
    }
	
	@Override
	protected RelDataType deriveRowType() {
        return hSchema.getRelDataType();
    }
	
	@Override
	public Operator<? extends OperatorDesc> attachedHiveOperator() {
		return rsOp;
	}

	@Override
	public Operator<? extends OperatorDesc> buildHiveOperator() {
		if (rsOp != null ) {
			return rsOp;
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public HiveSchema getSchema() {
		return hSchema;
	}

}
