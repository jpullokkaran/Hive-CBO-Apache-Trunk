package org.apache.hadoop.hive.ql.optimizer.optiq.schema;


import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.eigenbase.relopt.RelOptCluster;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ranges;

public class HiveSingleInputSchema extends HiveSchema {
	RowResolver rr;	
	ImmutableMap<String, ColInfo> internalnameToPosMap;
	
	protected HiveSingleInputSchema(RelOptCluster cluster, int offset, RowResolver rr) {
		super(cluster, Ranges.closedOpen(offset, offset + rr.getRowSchema().getSignature().size())); 
		this.rr = rr;
		ImmutableMap.Builder<String, ColInfo> b = new ImmutableMap.Builder<String, ColInfo>();
		
		int i=0;
		for(ColumnInfo ci : rr.getRowSchema().getSignature() ) {
			b.put(ci.getInternalName(), new ColInfo(i++, ci, rr.getInvRslvMap().get(ci.getInternalName())));
		}
		internalnameToPosMap = b.build();
	}
	
	protected HiveSingleInputSchema(RelOptCluster cluster, RowResolver rr) {
		this(cluster, 0, rr);
	}
	
	protected HiveSingleInputSchema(HiveSingleInputSchema orig, int offset) {
		super(orig.m_cluster, Ranges.closedOpen(offset, offset + orig.rr.getRowSchema().getSignature().size())); 
		rr = orig.rr;
		internalnameToPosMap = orig.internalnameToPosMap;
	}
	
	protected boolean isValidColumn(String cName) {
		return internalnameToPosMap.containsKey(cName);
	}
	
	protected ColInfo getColInfo(String iName) {
		return internalnameToPosMap.get(iName);
	}
	
	protected ColInfo getColInfo(int pos) {
		ColumnInfo ci = rr.getRowSchema().getSignature().get(pos - positionRange.lowerEndpoint()); 
		return getColInfo(ci.getInternalName());
	}	
	
	public HiveSchema move(int offset) {
		return new HiveSingleInputSchema(this, offset);
	}
}
