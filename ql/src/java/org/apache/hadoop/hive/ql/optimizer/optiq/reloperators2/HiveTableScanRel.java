package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators2;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCosts;
import org.apache.hadoop.hive.ql.optimizer.optiq.schema.HiveSchema;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptAbstractTable;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;

import static com.google.common.base.Preconditions.checkNotNull;

public class HiveTableScanRel extends TableAccessRelBase implements HiveRelNode {

	public static HiveTableScanRel create(RelOptCluster cluster, 
			RelOptSchema schema, 
			RelTraitSet traits,
			TableScanOperator tableScanOp, 
			RowResolver rr) {
		
		HiveSchema hSch = HiveSchema.createSchema(cluster, rr);
		String name = tableScanOp.getConf().getAlias();
		RelDataType dT = hSch.getRelDataType();
		
		RelOptHiveTable oTbl = new RelOptHiveTable(tableScanOp, 
				hSch,
				schema,
				name,
				dT);
		return new HiveTableScanRel(cluster, traits, oTbl);
	}

	HiveSchema hSchema;
	TableScanOperator tableScanOp;
	Statistics hiveStats;
	
	protected HiveTableScanRel(RelOptCluster cluster, RelTraitSet traits,
			RelOptHiveTable table) {
		super(cluster, traits, table);
		hSchema = table.hSchema;
		tableScanOp = table.tableScanOp;
		hiveStats = table.hiveStats;
	}
	
	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner) {
        double dRows = table.getRowCount();
        double dCpu = dRows * HiveCosts.PROCESSING_PER_UNIT;
        double dIo = dRows * HiveCosts.LOCAL_READ_PER_UNIT;
        return planner.makeCost(dRows, dCpu, dIo);
    }

	@Override
	public Operator<? extends OperatorDesc> attachedHiveOperator() {
		return tableScanOp;
	}

	@Override
	public Operator<? extends OperatorDesc> buildHiveOperator() {
		if (tableScanOp != null ) {
			return tableScanOp;
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public HiveSchema getSchema() {
		return hSchema;
	}

	public static class RelOptHiveTable extends RelOptAbstractTable {

		TableScanOperator tableScanOp;
		HiveSchema hSchema;
		Statistics hiveStats;
		
		protected RelOptHiveTable(
				TableScanOperator tableScanOp,
				HiveSchema hSch,
				RelOptSchema schema, 
				String name,
				RelDataType rowType) {
			super(schema, name, rowType);
			this.tableScanOp = tableScanOp;
			this.hSchema = hSch;
			checkNotNull(tableScanOp.getStatistics());
			hiveStats = tableScanOp.getStatistics();
		}
		
		@Override
		public double getRowCount() {
	        return hiveStats.getNumRows();
	    }
	}


}
