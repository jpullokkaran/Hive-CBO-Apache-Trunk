package org.apache.hadoop.hive.ql.optimizer.optiq.cost;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveIRShuffleRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.MapJoinStreamingRelation;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.OptiqStatsUtil;
import org.eigenbase.relopt.RelOptUtil;

public class HiveCostUtil {
	private static final double cpuCostInNanoSec = 1.0;
	private static final double netCostInNanoSec = 150 * cpuCostInNanoSec;
	private static final double localFSWriteCostInNanoSec = 4 * netCostInNanoSec;
	private static final double localFSReadCostInNanoSec = 4 * netCostInNanoSec;
	private static final double hDFSWriteCostInNanoSec = 10 * localFSWriteCostInNanoSec;
	private static final double hDFSReadCostInNanoSec = 1.5 * localFSReadCostInNanoSec;

	public static HiveCost computeCost(HiveTableScanRel t) {
		double cardinality = t.getRows();
		return new HiveCost(cardinality, 0, hDFSWriteCostInNanoSec
				* cardinality * t.getAvgTupleSize());
	}

	public static HiveCost computeCost(HiveFilterRel f) {
		double cardinality = f.getRows();
		return new HiveCost(cardinality, cpuCostInNanoSec * cardinality, 0);

	}

	public static HiveCost computeCost(HiveProjectRel s) {
		return new HiveCost(s.getRows(), 0, 0);
	}

	public static HiveCost computeCost(HiveIRShuffleRel sh) {
		double shuffleCardinality = sh.getRows();
		double shuffleAvgTupleSize = sh.getAvgTupleSize();
		double cpuCost = shuffleCardinality * Math.log(shuffleCardinality) * cpuCostInNanoSec;
		double ioCost = shuffleCardinality * shuffleAvgTupleSize * (localFSWriteCostInNanoSec + localFSReadCostInNanoSec + netCostInNanoSec);

		return new HiveCost(shuffleCardinality, cpuCost, ioCost);
	}

	public static HiveCost computeCost(HiveJoinRel j) {
		HiveRel leftRel = OptiqUtil.getNonSubsetRelNode(j.getLeft());
		HiveRel rightRel = OptiqUtil.getNonSubsetRelNode(j.getRight());
		double cpuCost = Double.MAX_VALUE;
		double ioCost = Double.MAX_VALUE;
		double joinCardinality = Double.MAX_VALUE;
		

		if (leftRel != null && rightRel != null) {
			double leftCardinality = leftRel.getRows();
			double leftAvgTupleSize = leftRel.getAvgTupleSize();
			double rightCardinality = rightRel.getRows();
			double rightAvgTupleSize = rightRel.getAvgTupleSize();
			joinCardinality = j.getRows();

			if (j.getJoinAlgorithm() == JoinAlgorithm.COMMON_JOIN || 
					j.getJoinAlgorithm() == JoinAlgorithm.NONE)  {
				cpuCost = (leftCardinality + rightCardinality) * cpuCostInNanoSec;
				// favor plans with the larger table to the right
				// cost of holding left rows in memory
				cpuCost += (leftCardinality * cpuCostInNanoSec);
				ioCost = 0;
				
				// hb: 1/24/14 for now give this choice a finite cost; so we can test plan generation logic
				// with all Join Alg rules turned off. 
				if (j.getJoinAlgorithm() == JoinAlgorithm.NONE ) {
					cpuCost *= 2;
				}
				
			} else if (j.getJoinAlgorithm() == JoinAlgorithm.MAP_JOIN || j.getJoinAlgorithm() == JoinAlgorithm.BUCKET_JOIN) {
				/*long hashTableReplication = 0;
				MapJoinStreamingRelation streamingSide = j.getMapJoinStreamingSide();
				double streamingRelCardinality = leftCardinality;
				double nonStreamingRelCardinality = rightCardinality;
				double nonStreamingRelAvgTupleSize = rightAvgTupleSize;
				
				if (streamingSide == MapJoinStreamingRelation.LEFT_RELATION) {
					streamingRelCardinality = leftCardinality;
				} else if (streamingSide == MapJoinStreamingRelation.RIGHT_RELATION) {
					nonStreamingRelCardinality = rightCardinality;
					nonStreamingRelAvgTupleSize = rightAvgTupleSize;
				} else {
					throw new RuntimeException ("Map Join has no streaming side");
				}
				
				cpuCost = nonStreamingRelCardinality + (streamingRelCardinality + nonStreamingRelCardinality) * cpuCostInNanoSec;
				
				if (j.getJoinAlgorithm() == JoinAlgorithm.MAP_JOIN)
					hashTableReplication = OptiqStatsUtil.computeDegreeOfParallelization(j);
				else if (j.getJoinAlgorithm() == JoinAlgorithm.BUCKET_JOIN)
					hashTableReplication = getBucketReplicationFactor(j, JoinAlgorithm.BUCKET_JOIN);

				
				ioCost = nonStreamingRelCardinality * nonStreamingRelAvgTupleSize * netCostInNanoSec * hashTableReplication;
				*/
			} else if (j.getJoinAlgorithm() == JoinAlgorithm.SMB_JOIN) {
				//cpuCost =
			}
		}

		return new HiveCost(joinCardinality, cpuCost, ioCost);
	}
}
