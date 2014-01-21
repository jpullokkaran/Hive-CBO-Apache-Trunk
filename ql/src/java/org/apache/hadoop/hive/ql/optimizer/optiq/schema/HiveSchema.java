package org.apache.hadoop.hive.ql.optimizer.optiq.schema;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.ASTUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexSlot;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

/*
 * represents the input schema of a Node in the Optiq Graph.
 * represented in the form of Hive data structures.
 */
public abstract class HiveSchema {
	RelOptCluster m_cluster;
	Range<Integer> positionRange;
	
	protected HiveSchema(RelOptCluster cluster, Range<Integer> positionRange) {
		this.m_cluster = cluster;
		this.positionRange = positionRange;
	}
	
	protected abstract ColInfo getColInfo(String iName);
	
	protected abstract ColInfo getColInfo(int pos);
	
	protected abstract boolean isValidColumn(String cName);
	
	protected boolean isValidPosition(int pos) {
		return positionRange.contains(pos);
	}
	
	public abstract HiveSchema move(int offset);
	
	
	public RelDataType getRelDataType() {
	    RexBuilder rexBuilder = m_cluster.getRexBuilder();
	    RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
	    List<RelDataType> oTypes = new LinkedList<RelDataType>();
	    List<String> oNames = new LinkedList<String>();
	    
	    for(int i = positionRange.lowerEndpoint(); i < positionRange.upperEndpoint(); i++) {
	    	ColInfo cI = getColInfo(i);
	    	RelDataType oType = TypeConverter.convert(cI.ci.getType(), dtFactory);
	    	oNames.add(cI.ci.getInternalName());
	    	oTypes.add(oType);
	    }	    
	    return dtFactory.createStructType(oTypes, oNames);
	  }
	
	public int getPosition(ExprNodeColumnDesc e) {
		checkNotNull(e);
		String iName = e.getColumn();
		return getColInfo(iName).getPos();
	}
	
	public RexNode toRex(ExprNodeColumnDesc e)  {
		RexBuilder rexBuilder = m_cluster.getRexBuilder();
	    RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
	    String iName = e.getColumn();
		ColInfo cI = getColInfo(iName);
		return m_cluster.getRexBuilder().makeInputRef(TypeConverter.convert(cI.ci.getType(), dtFactory), cI.getPos());		
	}
	
	public ExprNodeColumnDesc toExpr(RexNode r) {
		checkNotNull(r);
		checkArgument(r instanceof RexSlot, "can only convert input references to column expr nodes");
		int pos = ((RexSlot)r).getIndex();
		ColInfo cI = getColInfo(pos);
		return new ExprNodeColumnDesc(cI.ci.getType(), cI.ci.getInternalName(), 
				cI.ci.getTabAlias(), cI.ci.getIsVirtualCol());

	}
	
	public ASTNode toAST(RexNode r) {
		checkNotNull(r);
		checkArgument(r instanceof RexSlot, "can only convert input references to column expr nodes");
		int pos = ((RexSlot)r).getIndex();
		ColInfo cI = getColInfo(pos);
		return ASTUtils.createColRefAST(cI.alias[0], cI.alias[1]);
	}
	
	protected class ColInfo {
		private int pos;
		ColumnInfo ci;
		String[] alias;
		
		ColInfo(int pos, ColumnInfo ci, String[] alias) {
			this.pos = pos;
			this.ci = ci;
		}
		
		protected int getPos() {
			return pos + HiveSchema.this.positionRange.lowerEndpoint();
		}
	}
	
	public static HiveSchema createSchema(RelOptCluster cluster, int offset, RowResolver rr) {
		return new HiveSingleInputSchema(cluster, offset, rr);
	}
	
	public static HiveSchema createSchema(RelOptCluster cluster, RowResolver rr) {
		return createSchema(cluster, 0, rr);
	}
	
	public static HiveSchema createJoinSchema(RelOptCluster cluster, int offset, Object ...ins) {
		int currSize = offset;
		int i = 0;
		HiveSchema[] childSchemas = new HiveSchema[ins.length];
		for(Object o : ins) {
			if ( o instanceof RowResolver ) {
				childSchemas[i] = createSchema(cluster, currSize, (RowResolver) o);
			} else {
				childSchemas[i] = ((HiveSchema)o).move(currSize);
			}
			currSize = childSchemas[i].positionRange.upperEndpoint();
			i++;
		}
		
		return new HiveJoinSchema(cluster, Ranges.closedOpen(offset, currSize), childSchemas);
	}
	
	public static  HiveSchema createJoinSchema(RelOptCluster cluster, Object ...ins) {
		return createJoinSchema(cluster, 0, ins);
	}

}
