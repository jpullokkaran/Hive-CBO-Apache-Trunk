package org.apache.hadoop.hive.ql.optimizer.optiq.schema;

import org.eigenbase.relopt.RelOptCluster;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

public class HiveJoinSchema extends HiveSchema {
	
	ImmutableList<HiveSchema> inputs;

	protected HiveJoinSchema(RelOptCluster cluster, Range<Integer> positionRange, HiveSchema ...inputSchemas) {
		super(cluster, positionRange);
		inputs = ImmutableList.copyOf(inputSchemas);
	}
	
	protected HiveSchema getInput(String cName) {
		for (HiveSchema i : inputs) {
			if (i.isValidColumn(cName)) {
				return i;
			}
		}
		return null;
	}

	protected HiveSchema getInput(int pos) {
		for (HiveSchema i : inputs) {
			if (i.isValidPosition(pos)) {
				return i;
			}
		}
		return null;
	}
	
	protected boolean isValidColumn(String cName) {
		return getInput(cName) != null;
	}
	
	protected ColInfo getColInfo(String iName) {
		return getInput(iName).getColInfo(iName);
	}
	
	protected ColInfo getColInfo(int pos) {
		return getInput(pos).getColInfo(pos);
	}
	
	protected HiveSchema move(int offset) {
		HiveSchema[] ins = new HiveSchema[inputs.size()];
		for(int i=0; i < inputs.size(); i++) {
			ins[i] = inputs.get(i).move(offset);
		}
		return new HiveJoinSchema(m_cluster, 
				Ranges.closedOpen(offset, positionRange.upperEndpoint() + offset), 
				ins);
	}

}
