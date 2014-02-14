package org.apache.hadoop.hive.ql.optimizer.optiq.translator;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlOperator;

import com.google.common.collect.ImmutableMap;

public class RexNodeConverter {
	
	RelOptCluster m_cluster;
	RelDataType inpDataType;
	ImmutableMap<String, Integer> nameToPosMap;
	int offset;
	
	public RexNodeConverter(RelOptCluster m_cluster, RelDataType inpDataType, 
			ImmutableMap<String, Integer> nameToPosMap, int offset) {
		this.m_cluster = m_cluster;
		this.inpDataType = inpDataType;
		this.nameToPosMap = nameToPosMap;
		this.offset = offset;
	}

	public RexNode convert(ExprNodeDesc expr) {
		if (expr instanceof ExprNodeGenericFuncDesc) {
			return convert((ExprNodeGenericFuncDesc) expr);
		} else if (expr instanceof ExprNodeConstantDesc) {
			return convert((ExprNodeConstantDesc) expr);
		} else if (expr instanceof ExprNodeColumnDesc) {
			return convert((ExprNodeColumnDesc) expr);
		} else {
			throw new RuntimeException("Unsupported Expression");
		}
		//TODO: handle a) ExprNodeNullDesc b) ExprNodeFieldDesc c) ExprNodeColumnListDesc
	}
	
	
	private RexNode convert(final ExprNodeGenericFuncDesc func) {
		SqlOperator optiqOp = SqlFunctionConverter.getOptiqOperator(func.getGenericUDF());
		List<RexNode> childRexNodeLst = new LinkedList<RexNode>();

		for (ExprNodeDesc childExpr : func.getChildren()) {
			childRexNodeLst.add(convert(childExpr));
		}
		return m_cluster.getRexBuilder().makeCall(optiqOp, childRexNodeLst);
	}
	
	protected RexNode convert(ExprNodeColumnDesc col) {
		int pos = nameToPosMap.get(col.getColumn());
		return m_cluster.getRexBuilder().makeInputRef(inpDataType.getFieldList().get(pos).getType(), pos + offset);
	}
	
	protected RexNode convert(ExprNodeConstantDesc literal) {
		RexBuilder rexBuilder = m_cluster.getRexBuilder();
		RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
		PrimitiveTypeInfo hiveType = (PrimitiveTypeInfo) literal.getTypeInfo();
		RelDataType  optiqDataType = TypeConverter.convert(hiveType, dtFactory);
		
		PrimitiveCategory hiveTypeCategory = hiveType.getPrimitiveCategory();
		RexNode optiqLiteral = null;
		Object value = literal.getValue();

		// TODO: Verify if we need to use ConstantObjectInspector to unwrap data
		switch (hiveTypeCategory) {
		case BOOLEAN:
			optiqLiteral = rexBuilder.makeLiteral(((Boolean) value)
					.booleanValue());
			break;
		case BYTE:
			optiqLiteral = rexBuilder.makeExactLiteral(new BigDecimal(
					(Short) value));
			break;
		case SHORT:
			optiqLiteral = rexBuilder.makeExactLiteral(new BigDecimal(
					(Short) value));
			break;
		case INT:
			optiqLiteral = rexBuilder.makeExactLiteral(new BigDecimal(
					(Integer) value));
			break;
		case LONG:
			optiqLiteral = rexBuilder.makeBigintLiteral(new BigDecimal(
					(Long) value));
			break;
		// TODO: is Decimal an exact numeric or approximate numeric?
		case DECIMAL:
			optiqLiteral = rexBuilder.makeExactLiteral((BigDecimal) value);
			break;
		case FLOAT:
			optiqLiteral = rexBuilder.makeApproxLiteral(new BigDecimal(
					(Float) value), optiqDataType);
			break;
		case DOUBLE:
			optiqLiteral = rexBuilder.makeApproxLiteral(new BigDecimal(
					(Double) value), optiqDataType);
			break;
		case STRING:
			optiqLiteral = rexBuilder.makeLiteral((String) value);
			break;
		case DATE:
		case TIMESTAMP:
		case BINARY:
		case VOID:
		case UNKNOWN:
		default:
			throw new RuntimeException("UnSupported Literal");
		}

		return optiqLiteral;
	}

}
