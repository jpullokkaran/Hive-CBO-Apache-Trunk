package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Token;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexCorrelVariable;
import org.eigenbase.rex.RexDynamicParam;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexFieldCollation;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.eigenbase.rex.RexRangeRef;
import org.eigenbase.rex.RexShuttle;
import org.eigenbase.rex.RexVisitor;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.rex.RexWindow;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

public class OptiqRelExprToHiveASTConverter extends RexVisitorImpl<ASTNode> {

  private final RelNode m_exprRelNode;

  private final RexNode m_expr;

  private final List<RexNode> m_filterSchema;

  Map<String, Pair<String, String>> m_colTabMap;

  protected OptiqRelExprToHiveASTConverter(RelNode exprRel, RexNode expr,
      Map<String, Pair<String, String>> colTabMap) {
    super(true);
    m_exprRelNode = exprRel;
    m_expr = expr;
    m_colTabMap = colTabMap;
    if (m_colTabMap == null) {
      throw new RuntimeException("Column to TableName.ColumnName map can not be NULL");
    }
    m_filterSchema = null;
  }

  protected OptiqRelExprToHiveASTConverter(RelNode exprRel, List<RexNode> filterSchema,
      RexNode filter, Map<String, Pair<String, String>> colTabMap) {
    super(true);
    m_exprRelNode = exprRel;
    m_filterSchema = filterSchema;
    m_expr = filter;
    m_colTabMap = colTabMap;
    if (m_colTabMap == null) {
      throw new RuntimeException("Column to TableName.ColumnName map can not be NULL");
    }
  }

  @Override
  public ASTNode visitInputRef(RexInputRef inputRef)
  {
    ASTNode astNode = null;
    astNode = OptiqRelToHiveASTConverter.getTableDotColumnName(m_colTabMap
        .get(m_exprRelNode.getRowType().getFieldNames().get(inputRef.getIndex())));

    return astNode;
  }

  @Override
  public ASTNode visitLocalRef(RexLocalRef localRef)
  {
    ASTNode astNode = null;
    if (m_filterSchema == null) {
      astNode = OptiqRelToHiveASTConverter.getTableDotColumnName(m_colTabMap
          .get(m_exprRelNode.getRowType().getFieldNames().get(localRef.getIndex())));
    } else {
      astNode = m_filterSchema.get(localRef.getIndex()).accept(this);
    }

    return astNode;
  }

  @Override
  public ASTNode visitLiteral(RexLiteral literal)
  {
    CommonToken literalToken = null;
    switch (literal.getType().getSqlTypeName()) {
    case BOOLEAN: {
    }
      ;
      break;
    case TINYINT: {
      literalToken = new CommonToken(HiveParser.TinyintLiteral, String.valueOf(literal.getValue3()));
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
      literalToken = new CommonToken(HiveParser.BigintLiteral, String.valueOf(literal.getValue3()));
    }
      ;
      break;
    default:
      throw new RuntimeException("Couldn't find type");
    }

    return new ASTNode(literalToken);
  }

  @Override
  public ASTNode visitOver(RexOver over)
  {
    ASTNode r = visitCall(over);
    if (!deep) {
      return null;
    }
    final RexWindow window = over.getWindow();
    for (RexFieldCollation orderKey : window.orderKeys) {
      orderKey.left.accept(this);
    }
    for (RexNode partitionKey : window.partitionKeys) {
      partitionKey.accept(this);
    }
    return r;
  }

  @Override
  public ASTNode visitCorrelVariable(RexCorrelVariable correlVariable)
  {
    return null;
  }

  @Override
  public ASTNode visitCall(RexCall call)
  {
    ASTNode r;

    if (!deep) {
      return null;
    }

    ASTNode astNode = null;
    List<ASTNode> astNodeLst = new LinkedList<ASTNode>();
    for (RexNode operand : call.operands) {
      astNode = operand.accept(this);
      astNodeLst.add(astNode);
    }

    r = new ASTNode(getCallToken(call.getOperator()));
    r.addChildren(astNodeLst);

    return r;
  }

  @Override
  public ASTNode visitDynamicParam(RexDynamicParam dynamicParam)
  {
    return null;
  }

  @Override
  public ASTNode visitRangeRef(RexRangeRef rangeRef)
  {
    return new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, rangeRef.getType()
        .getFieldNames().get(rangeRef.getOffset())));
  }

  @Override
  public ASTNode visitFieldAccess(RexFieldAccess fieldAccess)
  {
    if (!deep) {
      return null;
    }
    final RexNode expr = fieldAccess.getReferenceExpr();
    return expr.accept(this);
  }

  /**
   * <p>
   * Visits an array of expressions, returning the logical 'and' of their results.
   *
   * <p>
   * If any of them returns false, returns false immediately; if they all return true, returns true.
   *
   * @see #visitArrayOr
   * @see RexShuttle#visitArray
   */
  public static boolean visitArrayAnd(
      RexVisitor<Boolean> visitor,
      List<RexNode> exprs)
  {
    for (RexNode expr : exprs) {
      final boolean b = expr.accept(visitor);
      if (!b) {
        return false;
      }
    }
    return true;
  }

  /**
   * <p>
   * Visits an array of expressions, returning the logical 'or' of their results.
   *
   * <p>
   * If any of them returns true, returns true immediately; if they all return false, returns false.
   *
   * @see #visitArrayAnd
   * @see RexShuttle#visitArray
   */
  public static boolean visitArrayOr(
      RexVisitor<Boolean> visitor,
      List<RexNode> exprs)
  {
    for (RexNode expr : exprs) {
      final boolean b = expr.accept(visitor);
      if (b) {
        return true;
      }
    }
    return false;
  }

  private Token getCallToken(SqlOperator call) {
    Token callToken = null;

    if (call == SqlStdOperatorTable.EQUALS) {
      callToken = new CommonToken(HiveParser.EQUAL, "=");
    }

    return callToken;
  }

  public static ASTNode astExpr(RelNode exprRel, RexNode expr,
      Map<String, Pair<String, String>> colTabMap)
  {
    return expr.accept(new OptiqRelExprToHiveASTConverter(exprRel, expr, colTabMap));
  }

  public static ASTNode astExpr(RelNode exprRel, List<RexNode> filterSchema, RexNode filter,
      Map<String, Pair<String, String>> colTabMap)
  {
    return filter.accept(new OptiqRelExprToHiveASTConverter(exprRel, filterSchema, filter, colTabMap));
  }

}
