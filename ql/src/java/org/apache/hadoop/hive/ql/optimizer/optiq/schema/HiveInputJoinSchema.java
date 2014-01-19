package org.apache.hadoop.hive.ql.optimizer.optiq.schema;

import org.eigenbase.relopt.RelOptCluster;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

public class HiveInputJoinSchema extends HiveInputSchema {
	
	ImmutableList<HiveInputSchema> inputs;

	protected HiveInputJoinSchema(RelOptCluster cluster, Range<Integer> positionRange, HiveInputSchema ...inputSchemas) {
		super(cluster, positionRange);
		inputs = ImmutableList.copyOf(inputSchemas);
	}
	
	protected HiveInputSchema getInput(String cName) {
		for (HiveInputSchema i : inputs) {
			if (i.isValidColumn(cName)) {
				return i;
			}
		}
		return null;
	}

	protected HiveInputSchema getInput(int pos) {
		for (HiveInputSchema i : inputs) {
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
	
	protected HiveInputSchema move(int offset) {
		HiveInputSchema[] ins = new HiveInputSchema[inputs.size()];
		for(int i=0; i < inputs.size(); i++) {
			ins[i] = inputs.get(i).move(offset);
		}
		return new HiveInputJoinSchema(m_cluster, 
				Ranges.closedOpen(offset, positionRange.upperEndpoint() + offset), 
				ins);
	}

}
