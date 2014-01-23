package org.apache.hadoop.hive.ql.optimizer.optiq.cost;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;

public class HiveCostUtil {
	private static final double cpuCostInNanoSec = 1.0;
	private static final double netCostInNanoSec = 150 * cpuCostInNanoSec;
	private static final double localFSWriteCostInNanoSec = 4 * netCostInNanoSec;
	private static final double localFSReadCostInNanoSec = 4 * netCostInNanoSec;
	private static final double hDFSWriteCostInNanoSec = 10 * localFSWriteCostInNanoSec;
	private static final double hDFSReadCostInNanoSec = 1.5 * localFSReadCostInNanoSec;

	public static HiveCost computeCost(HiveTableScanRel t) {
		double cardinality = t.getRows();
		return new HiveCost(cardinality, 0, hDFSWriteCostInNanoSec * cardinality * t.getAvgTupleSize());
	}

	public static HiveCost computeCost(HiveFilterRel f) {
		double cardinality = f.getRows();
		return new HiveCost(cardinality, cpuCostInNanoSec * cardinality, 0);

	}

	public static HiveCost computeCost(HiveProjectRel s) {
		return new HiveCost(s.getRows(), 0, 0);
	}

	public static HiveCost computeCost(HiveJoinRel j) {
		double leftCardinality = OptiqUtil.getNonSubsetRelNode(j.getLeft())
				.getRows();
		double leftAvgTupleSize = OptiqUtil.getNonSubsetRelNode(j.getLeft())
				.getAvgTupleSize();
		double rightCardinality = OptiqUtil.getNonSubsetRelNode(j.getRight())
				.getRows();
		double rightAvgTupleSize = OptiqUtil.getNonSubsetRelNode(j.getRight())
				.getAvgTupleSize();
		double cpuCost = Double.MAX_VALUE;
		double ioCost = Double.MAX_VALUE;

		if (j.getJoinAlgorithm() == JoinAlgorithm.COMMON_JOIN) {
			cpuCost = leftCardinality * Math.log(leftCardinality)
					* cpuCostInNanoSec + rightCardinality
					* Math.log(rightCardinality) * cpuCostInNanoSec
					+ (leftCardinality + rightCardinality) * cpuCostInNanoSec;
			ioCost = (leftCardinality * leftAvgTupleSize + rightCardinality * rightAvgTupleSize) * (localFSWriteCostInNanoSec + localFSReadCostInNanoSec + netCostInNanoSec);
		} else if (j.getJoinAlgorithm() == JoinAlgorithm.MAP_JOIN) {
			
		}
		
		return new HiveCost(j.getRows(), cpuCost, ioCost);
	}
}
