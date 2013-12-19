package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import net.hydromatic.optiq.Schema;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEncode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArray;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayContains;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAssertTrue;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCoalesce;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcatWS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEWAHBitmapAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEWAHBitmapEmpty;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEWAHBitmapOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFElt;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFField;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFFormatNumber;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFFromUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInstr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLocate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMap;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMapKeys;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMapValues;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNamedStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNvl;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFReflect;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFReflect2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSentences;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSize;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSortArray;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSplit;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStringToMap;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTranslate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnion;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.ql.udf.xml.GenericUDFXPath;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlBinaryOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

public class HiveToOptiqRelConverter {
  final SemanticAnalyzer m_semanticAnalyzer;
  final RelOptCluster m_cluster;
  final RelOptSchema m_relOptSchema;
  final Schema m_schema;
  private final HiveConf m_conf;


  HiveToOptiqRelConverter(SemanticAnalyzer semAnalyzer, RelOptCluster relOptCluster,
      RelOptSchema relOptSchema, Schema schema, HiveConf conf) {
    m_semanticAnalyzer = semAnalyzer;
    m_cluster = relOptCluster;
    m_relOptSchema = relOptSchema;
    m_schema = schema;
    m_conf = conf;
  }


  public RelNode convertOpDAG(
      final Operator op)
  {
    RelNode relNode = null;
    List<Operator<? extends OperatorDesc>> parentOpsInHiveDAG = op.getParentOperators();
    List<RelNode> childRelNodesInOptiqRelTree = null;
    if (parentOpsInHiveDAG != null && !parentOpsInHiveDAG.isEmpty()) {
      childRelNodesInOptiqRelTree = new LinkedList<RelNode>();
      for (Operator<? extends OperatorDesc> opInHiveDAG : parentOpsInHiveDAG) {
        RelNode rel = convertOpDAG(opInHiveDAG);
        if (rel != null) {
          childRelNodesInOptiqRelTree.add(rel);
        }
      }
    }

    relNode = convertOp(childRelNodesInOptiqRelTree, op);
    // TODO: relNode could be null for Terminal OPs (FS/RS/HTS) & SCRIPT/TRANSFORM. Currently we
    // assume these ops will have only one parent. This needs to be revisited.
    if (relNode == null && childRelNodesInOptiqRelTree != null) {
      relNode = childRelNodesInOptiqRelTree.get(0);
    }

    return relNode;
  }

  private RelNode convertOp(
      final List<RelNode> childRelNodesInOptiqRelTree, final Operator op) {
    switch (op.getType()) {
    case SELECT:
      return convertSelectOp(childRelNodesInOptiqRelTree,
          (SelectOperator) op);
    case FILTER:
      return convertFilterOp(childRelNodesInOptiqRelTree,
          (FilterOperator) op);
    case TABLESCAN:
      return convertTableOp(childRelNodesInOptiqRelTree,
          (TableScanOperator) op);
    case JOIN:
      return convertJoinOp(childRelNodesInOptiqRelTree,
          (JoinOperator) op);
    default:
      break;
    }

    return null;
  }

  private TableAccessRelBase convertTableOp(
      final List<RelNode> childRelNodesInOptiqRelTree,
      final TableScanOperator tableScanOp) {
    TableAccessRelBase tableRel = null;

    RelDataType rowType = HiveToOptiqTypeConverter.getType(m_cluster, tableScanOp);
    RelOptHiveTable optTable = new RelOptHiveTable(m_relOptSchema, tableScanOp.getConf()
        .getAlias(), rowType, m_semanticAnalyzer.getTable(tableScanOp), tableScanOp.getSchema(), m_conf);

    tableRel = new HiveTableScanRel(
        m_cluster,
        m_cluster.traitSetOf(HiveRel.CONVENTION),
        optTable,  rowType);

    return tableRel;
  }

  private HiveFilterRel convertFilterOp(final List<RelNode> childRelNodesInOptiqRelTree,
      final FilterOperator filterOp) {
    RexNode convertedFilterExpr = convertToOptiqExpr(filterOp.getConf().getPredicate(),
        getNodeThatCanBeTranslatedToOptiq(filterOp
            .getParentOperators().get(0)), childRelNodesInOptiqRelTree.get(0));

    return new HiveFilterRel(m_cluster, m_cluster.traitSetOf(HiveRel.CONVENTION), childRelNodesInOptiqRelTree.get(0), convertedFilterExpr);
  }

  private HiveProjectRel convertSelectOp(
      final List<RelNode> childRelNodesInOptiqRelTree,
      final SelectOperator selectOp) {
    RelNode inputRelNode = childRelNodesInOptiqRelTree.get(0);
    List<ExprNodeDesc> colLst = selectOp.getConf().getColList();
    List<RexNode> optiqColLst = new LinkedList<RexNode>();

    Operator hiveParentOp = getNodeThatCanBeTranslatedToOptiq(selectOp.getParentOperators().get(0));
    RelNode optiqChildRelNode = childRelNodesInOptiqRelTree.get(0);
    for (ExprNodeDesc colExpr : colLst) {
      optiqColLst.add(convertToOptiqExpr(colExpr, hiveParentOp, optiqChildRelNode));
    }

    return new HiveProjectRel(m_cluster, inputRelNode, optiqColLst, selectOp.getConf()
        .getOutputColumnNames(), 0);
  }

  private static Operator getNodeThatCanBeTranslatedToOptiq(Operator op) {
    OperatorType t = op.getType();
    switch (t) {
    case HASHTABLESINK:
    case HASHTABLEDUMMY:
    case FILESINK:
    case REDUCESINK:
    case EXTRACT:
      return getNodeThatCanBeTranslatedToOptiq((Operator) op.getParentOperators().get(0));
    }

    return op;
  }

  private RelNode convertJoinOp(
      List<RelNode> childRelNodesInOptiqRelTree, JoinOperator op, JoinCondDesc jc) {
    RelNode joinRel = null;
    Operator leftParent = op.getParentOperators().get(jc.getLeft());
    Operator rightParent = op.getParentOperators().get(jc.getRight());
    RelNode leftRel = childRelNodesInOptiqRelTree.get(0);
    RelNode rightRel = childRelNodesInOptiqRelTree.get(1);

    if (leftParent instanceof ReduceSinkOperator && rightParent instanceof ReduceSinkOperator) {
      List<ExprNodeDesc> leftCols = ((ReduceSinkDesc) (leftParent.getConf())).getKeyCols();
      List<ExprNodeDesc> rightCols = ((ReduceSinkDesc) (rightParent.getConf())).getKeyCols();
      RexNode joinPredicate = null;
      JoinRelType joinType = JoinRelType.INNER;

      // TODO: what about semi join
      switch (jc.getType()) {
      case JoinDesc.INNER_JOIN:
        joinType = JoinRelType.INNER;
        break;
      case JoinDesc.LEFT_OUTER_JOIN:
        joinType = JoinRelType.LEFT;
        break;
      case JoinDesc.RIGHT_OUTER_JOIN:
        joinType = JoinRelType.RIGHT;
        break;
      case JoinDesc.FULL_OUTER_JOIN:
        joinType = JoinRelType.FULL;
        break;
      }

      int i = 0;
      for (ExprNodeDesc expr : leftCols) {
        List<RexNode> eqExpr = new LinkedList<RexNode>();
        if (expr instanceof ExprNodeColumnDesc) {
          eqExpr.add(convertToOptiqColumn((ExprNodeColumnDesc) expr, (Operator) leftParent
              .getParentOperators().get(0), leftRel, 0));
          eqExpr.add(convertToOptiqColumn((ExprNodeColumnDesc) rightCols.get(i),
              (Operator) rightParent.getParentOperators().get(0), rightRel, leftRel.getRowType()
                  .getFieldCount()));
        }
        RexNode eqOp = m_cluster.getRexBuilder().makeCall(SqlStdOperatorTable.equalsOperator,
            eqExpr);
        i++;

        if (joinPredicate == null) {
          joinPredicate = eqOp;
        }
        else
        {
          List<RexNode> conjElements = new LinkedList<RexNode>();
          conjElements.add(joinPredicate);
          conjElements.add(eqOp);
          joinPredicate = m_cluster.getRexBuilder().makeCall(SqlStdOperatorTable.andOperator,
              conjElements);
        }
      }
      joinRel = HiveJoinRel.getJoin(m_cluster, leftRel, rightRel, joinPredicate, joinType);
    } else {
      throw new RuntimeException("Right & Left of Join Condition columns are not equal");
    }

    return joinRel;
  }

  private RelNode convertJoinOp(
      List<RelNode> childRelNodesInOptiqRelTree, JoinOperator op) {
    JoinCondDesc[] condLst = op.getConf().getConds();
    RelNode joinNode = null;

    for (JoinCondDesc jc : condLst) {
      joinNode = convertJoinOp(childRelNodesInOptiqRelTree, op, jc);
      break;
      // TODO we break here because currently we assume single Join. We need to support multi join
      // in single join op
    }

    return joinNode;
  }

  private RexNode convertToOptiqExpr(final ExprNodeDesc expr, final Operator hiveOP,
      final RelNode optiqOP) {
    if (expr instanceof ExprNodeGenericFuncDesc) {
      return convertToOptiqFunc((ExprNodeGenericFuncDesc) expr, hiveOP, optiqOP);
    } else if (expr instanceof ExprNodeConstantDesc) {
      return convertToOptiqLiteral((ExprNodeConstantDesc) expr, optiqOP);
    } else if (expr instanceof ExprNodeColumnDesc) {
      return convertToOptiqColumn((ExprNodeColumnDesc) expr, hiveOP, optiqOP, 0);
    } else {
      throw new RuntimeException("Unsupported Expression");
    }
  }

  private static List<ExprNodeDesc> getTypeSafeChildExprs(final ExprNodeGenericFuncDesc func) {
    List<ExprNodeDesc> childExprs = func.getChildren();

    // TODO: We may need to make "Between" and "Case" type safe
    if (func.getGenericUDF() instanceof GenericUDFBaseCompare) {
      if (childExprs.size() != 2) {
        throw new RuntimeException("Unsupported Expression");
      }

      ExprNodeDesc newExpr;
      List<ExprNodeDesc> modifiedChildExprs = new LinkedList<ExprNodeDesc>();
      TypeInfo commonType = FunctionRegistry.getCommonClassForComparison(childExprs.get(0)
          .getTypeInfo(),
          childExprs.get(1).getTypeInfo());

      for (ExprNodeDesc funcArg : childExprs) {
        newExpr = funcArg;
        if (TypeInfoUtils.isConversionRequiredForComparison(funcArg.getTypeInfo(), commonType)) {
          try {
            newExpr = ParseUtils.createConversionCast(funcArg,
                (PrimitiveTypeInfo) commonType);
          } catch (SemanticException e) {
            throw new RuntimeException("Unsupported Expression", e);
          }
        }
        modifiedChildExprs.add(newExpr);

      }
      childExprs = modifiedChildExprs;
    }

    return childExprs;
  }

  private RexNode convertToOptiqFunc(final ExprNodeGenericFuncDesc func,
      final Operator hiveOP, final RelNode optiqOP) {
    List<RexNode> childRexNodeLst = new LinkedList<RexNode>();
    List<ExprNodeDesc> childExprs = getTypeSafeChildExprs(func);

    for (ExprNodeDesc childExpr : childExprs) {
      childRexNodeLst.add(convertToOptiqExpr(childExpr, hiveOP, optiqOP));
    }

    return convertToOptiqUDF(func.getGenericUDF(), m_cluster.getRexBuilder(), childRexNodeLst);
  }


  private RexNode convertToOptiqLiteral(ExprNodeConstantDesc literal, final RelNode optiqOP) {
    RexBuilder rexBuilder = m_cluster.getRexBuilder();
    RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
    PrimitiveTypeInfo hiveLiteralType = (PrimitiveTypeInfo) literal.getTypeInfo();
    RelDataType optiqLiteralType = HiveToOptiqTypeConverter.convert(hiveLiteralType, dtFactory);
    PrimitiveCategory hiveLiteralTypeCategory = hiveLiteralType.getPrimitiveCategory();
    RexNode optiqLiteral = null;
    Object value = literal.getValue();

    // TODO: Verify if we need to use ConstantObjectInspector to unwrap data
    switch (hiveLiteralTypeCategory) {
    case BOOLEAN:
      optiqLiteral = rexBuilder.makeLiteral(((Boolean) value).booleanValue());
      break;
    case BYTE:
      optiqLiteral = rexBuilder.makeExactLiteral(new BigDecimal((Short) value));
      break;
    case SHORT:
      optiqLiteral = rexBuilder.makeExactLiteral(new BigDecimal((Short) value));
      break;
    case INT:
      optiqLiteral = rexBuilder.makeExactLiteral(new BigDecimal((Integer) value));
      break;
    case LONG:
      optiqLiteral = rexBuilder.makeBigintLiteral(new BigDecimal((Long) value));
      break;
    // TODO: is Decimal an exact numeric or approximate numeric?
    case DECIMAL:
      optiqLiteral = rexBuilder.makeExactLiteral((BigDecimal) value);
      break;
    case FLOAT:
      optiqLiteral = rexBuilder.makeApproxLiteral(new BigDecimal((Float) value), optiqLiteralType);
      break;
    case DOUBLE:
      optiqLiteral = rexBuilder.makeApproxLiteral(new BigDecimal((Double) value), optiqLiteralType);
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

  private RexNode convertToOptiqColumn(ExprNodeColumnDesc col, final Operator hiveOP,
      final RelNode optiqOP,
      int offset) {
    int pos = m_semanticAnalyzer.getRowResolver(hiveOP).getPositionDiscardingHiddenColumns(
        col.getColumn());
    return m_cluster.getRexBuilder().makeInputRef(
        optiqOP.getRowType().getFieldList().get(pos).getType(), pos + offset);
  }


  private RexNode convertToOptiqUDF(final GenericUDF func, RexBuilder rexBuilder, List<RexNode> args) {
    SqlBinaryOperator op = null;

    if (func instanceof GenericUDFEWAHBitmapAnd) {
      op = null;
    } else if (func instanceof GenericUDFEWAHBitmapOr) {

    } else if (func instanceof GenericUDFReflect) {

    } else if (func instanceof GenericUDFReflect2) {

    } else if (func instanceof GenericUDFArray) {

    } else if (func instanceof GenericUDFArrayContains) {

    } else if (func instanceof GenericUDFAssertTrue) {

    } else if (func instanceof GenericUDFOPEqualNS) {

    } else if (func instanceof GenericUDFOPEqual) {

    } else if (func instanceof GenericUDFOPEqualOrGreaterThan) {

    } else if (func instanceof GenericUDFOPEqualOrLessThan) {

    } else if (func instanceof GenericUDFOPGreaterThan) {

    } else if (func instanceof GenericUDFOPLessThan) {

    } else if (func instanceof GenericUDFOPNotEqual) {

    } else if (func instanceof GenericUDFBetween) {

    } else if (func instanceof GenericUDFBridge) {

    } else if (func instanceof GenericUDFCase) {

    } else if (func instanceof GenericUDFCoalesce) {

    } else if (func instanceof GenericUDFConcat) {

    } else if (func instanceof GenericUDFConcatWS) {

    } else if (func instanceof GenericUDFElt) {

    } else if (func instanceof GenericUDFEncode) {

    } else if (func instanceof GenericUDFEWAHBitmapEmpty) {

    } else if (func instanceof GenericUDFField) {

    } else if (func instanceof GenericUDFFormatNumber) {

    } else if (func instanceof GenericUDFToUtcTimestamp) {

    } else if (func instanceof GenericUDFFromUtcTimestamp) {

    } else if (func instanceof GenericUDFHash) {

    } else if (func instanceof GenericUDFIf) {

    } else if (func instanceof GenericUDFInstr) {

    } else if (func instanceof GenericUDFLeadLag) {

    } else if (func instanceof GenericUDFLocate) {

    } else if (func instanceof GenericUDFLower) {

    } else if (func instanceof GenericUDFMacro) {

    } else if (func instanceof GenericUDFMap) {

    } else if (func instanceof GenericUDFMapKeys) {

    } else if (func instanceof GenericUDFEWAHBitmapAnd) {

    } else if (func instanceof GenericUDFMapValues) {

    } else if (func instanceof GenericUDFNamedStruct) {

    } else if (func instanceof GenericUDFNvl) {

    } else if (func instanceof GenericUDFOPAnd) {

    } else if (func instanceof GenericUDFOPNot) {

    } else if (func instanceof GenericUDFOPNotNull) {

    } else if (func instanceof GenericUDFOPNull) {

    } else if (func instanceof GenericUDFOPOr) {

    } else if (func instanceof GenericUDFPrintf) {

    } else if (func instanceof GenericUDFSentences) {

    } else if (func instanceof GenericUDFSize) {

    } else if (func instanceof GenericUDFSortArray) {

    } else if (func instanceof GenericUDFSplit) {

    } else if (func instanceof GenericUDFStringToMap) {

    } else if (func instanceof GenericUDFStruct) {

    } else if (func instanceof GenericUDFTimestamp) {

    } else if (func instanceof GenericUDFToBinary) {

    } else if (func instanceof GenericUDFToDate) {

    } else if (func instanceof GenericUDFToDecimal) {

    } else if (func instanceof GenericUDFUnixTimeStamp) {

    } else if (func instanceof GenericUDFToUnixTimeStamp) {

    } else if (func instanceof GenericUDFToVarchar) {

    } else if (func instanceof GenericUDFTranslate) {

    } else if (func instanceof GenericUDFUnion) {

    } else if (func instanceof GenericUDFUpper) {

    } else if (func instanceof GenericUDFWhen) {

    } else if (func instanceof GenericUDFXPath) {

    }


    return rexBuilder.makeCall(SqlStdOperatorTable.equalsOperator, args);

    // return rexBuilder.makeCall(SqlStdOperatorTable.greaterThanOperator, args);
    // rexBuilder.makeCall(SqlStdOperatorTable.lessThanOperator, args);
    // rexBuilder.makeCall(SqlStdOperatorTable.equalsOperator, args);
  }


  public static RelNode convertOpDAG(RelOptCluster cluster, RelOptSchema relOptSchema,
      Schema schema, Operator sinkOp, SemanticAnalyzer semanticAnalyzer, HiveConf conf) {
    HiveToOptiqRelConverter hiveToOptiqconv = new HiveToOptiqRelConverter(semanticAnalyzer,
        cluster, relOptSchema, schema, conf);

    return hiveToOptiqconv.convertOpDAG(sinkOp);
  }
}
