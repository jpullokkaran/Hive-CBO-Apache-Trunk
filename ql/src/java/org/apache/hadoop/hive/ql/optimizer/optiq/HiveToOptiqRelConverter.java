package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import net.hydromatic.optiq.Schema;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.optiq.expr.RexNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.schema.TypeConverter;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
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
    
    List<String> neededCols = tableScanOp.getNeededColumns();

    RelDataType rowType = TypeConverter.getType(m_cluster, m_semanticAnalyzer.getRowResolver(tableScanOp), neededCols);
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
        .getOutputColumnNames(), ProjectRel.Flags.Boxed);
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
      int rightColOffSet = leftRel.getRowType().getFieldCount();

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
        eqExpr.add(convertToOptiqExpr(expr, 
        		(Operator<? extends OperatorDesc>) leftParent.getParentOperators().get(0),
            leftRel,
            0));
        eqExpr.add(convertToOptiqExpr(rightCols.get(i), 
        		(Operator<? extends OperatorDesc>) rightParent.getParentOperators().get(0),
        		rightRel,
        		rightColOffSet));
        
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
      
      // Translate non-joinkey predicate
      Set<Entry<Byte, List<ExprNodeDesc>>> filterExprSet = op.getConf().getFilters().entrySet();
      if (filterExprSet != null && !filterExprSet.isEmpty()) {
    	  RexNode eqExpr;
    	  int colOffSet = 0;
    	  RelNode childRel;
    	  Operator parentHiveOp;
    	  int inputId;
    	  
    	  for (Entry<Byte, List<ExprNodeDesc>> entry : filterExprSet) {
    		  inputId = entry.getKey().intValue();
    		  if (inputId == 0) {
    			  colOffSet = 0;
    			  childRel = leftRel;
    			  parentHiveOp = leftParent;
    		  }
    		  else if (inputId == 1) {
    			  colOffSet = rightColOffSet;
    			  childRel = rightRel;
    			  parentHiveOp = rightParent;
    		  } else {
    			  throw new RuntimeException("Invalid Join Input");
    		  }
    		  
    		  for (ExprNodeDesc expr : entry.getValue()) {
    			  eqExpr = convertToOptiqExpr(expr, parentHiveOp, childRel, colOffSet);
    	          List<RexNode> conjElements = new LinkedList<RexNode>();
    	          conjElements.add(joinPredicate);
    	          conjElements.add(eqExpr);
    	          joinPredicate = m_cluster.getRexBuilder().makeCall(SqlStdOperatorTable.andOperator,
    	              conjElements);
    		  }
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
  
  private RexNode convertToOptiqExpr(final ExprNodeDesc expr, 
		  final Operator<? extends OperatorDesc> hiveOP,
      final RelNode optiqOP) {    
    return RexNodeConverter.convert(expr, 
    		m_cluster, 
    		m_semanticAnalyzer.getRowResolver(hiveOP), 
    		optiqOP.getRowType(),
    		0);
  }

  private RexNode convertToOptiqExpr(final ExprNodeDesc expr, 
		  final Operator<? extends OperatorDesc> hiveOP,
      final RelNode optiqOP,
      int offset) {    
    return RexNodeConverter.convert(expr, 
    		m_cluster, 
    		m_semanticAnalyzer.getRowResolver(hiveOP), 
    		optiqOP.getRowType(),
    		offset);
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

  public static RelNode convertOpDAG(RelOptCluster cluster, RelOptSchema relOptSchema,
      Schema schema, Operator sinkOp, SemanticAnalyzer semanticAnalyzer, HiveConf conf) {
    HiveToOptiqRelConverter hiveToOptiqconv = new HiveToOptiqRelConverter(semanticAnalyzer,
        cluster, relOptSchema, schema, conf);

    return hiveToOptiqconv.convertOpDAG(sinkOp);
  }
}
