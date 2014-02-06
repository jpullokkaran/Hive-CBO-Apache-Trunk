package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.ql.optimizer.optiq.expr.SqlFunctionConverter;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.SqlOperator;

public class OptiqRelExprToHiveASTConverter extends RexVisitorImpl<ASTNode> {

	private final RelNode m_exprRelNode;
	Map<String, Pair<String, String>> m_colTabMap;

	protected OptiqRelExprToHiveASTConverter(RelNode exprRel, RexNode expr,
	    Map<String, Pair<String, String>> colTabMap) {
		super(true);
		m_exprRelNode = exprRel;
		m_colTabMap = colTabMap;
		if (m_colTabMap == null) {
			throw new RuntimeException(
			    "Column to TableName.ColumnName map can not be NULL");
		}
	}

	@Override
	public ASTNode visitInputRef(RexInputRef inputRef) {
		ASTNode astNode = null;
		astNode = OptiqRelToHiveASTConverter.getTableDotColumnName(m_colTabMap
		    .get(m_exprRelNode.getRowType().getFieldNames()
		        .get(inputRef.getIndex())));

		return astNode;
	}

	@Override
	public ASTNode visitLiteral(RexLiteral literal) {
		CommonToken literalToken = null;
		switch (literal.getType().getSqlTypeName()) {
		case BOOLEAN: {
		}
			;
			break;
		case TINYINT: {
			literalToken = new CommonToken(HiveParser.TinyintLiteral,
			    String.valueOf(literal.getValue3()));
		}
			;
			break;
		case SMALLINT: {
			literalToken = new CommonToken(HiveParser.SmallintLiteral,
			    String.valueOf(literal.getValue3()));
		}
			;
			break;
		case INTEGER: {
			literalToken = new CommonToken(HiveParser.BigintLiteral,
			    String.valueOf(literal.getValue3()));
		}
			;
			break;
		default:
			throw new RuntimeException("Couldn't find type");
		}

		return new ASTNode(literalToken);
	}

	@Override
	public ASTNode visitCall(RexCall call) {
		if (!deep) {
			return null;
		}

		SqlOperator op = call.getOperator();
		List<ASTNode> astNodeLst = new LinkedList<ASTNode>();
		for (RexNode operand : call.operands) {
			astNodeLst.add(operand.accept(this));
		}
		return SqlFunctionConverter.buildAST(op, astNodeLst);

	}

	public static ASTNode astExpr(RelNode exprRel, RexNode expr,
	    Map<String, Pair<String, String>> colTabMap) {
		return expr.accept(new OptiqRelExprToHiveASTConverter(exprRel, expr,
		    colTabMap));
	}
}
