package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import net.hydromatic.optiq.rules.java.JavaRules.EnumerableCalcRel;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.eigenbase.rel.AggregateRel;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;

public class OptiqRelToHiveASTConverter {
  final AtomicLong m_tabCounter = new AtomicLong(0);
  final Random m_rand = new Random();

  // TODO: include Window/PTF operator
  private static class OptiqNodesForHiveSelectStmt {
    private SingleRel m_selectNode;
    private RelNode m_fromNode;
    // TODO: revisit this, seems hacky
    // Transient schema to assemble the final one (m_colTabMap)
    private final Map<RelNode, Map<String, Pair<String, String>>> m_opToColTabMap = new HashMap<RelNode, Map<String, Pair<String, String>>>();
    private Map<String, Pair<String, String>> m_colTabMap;
    private FilterRelBase m_filterNode;
    private AggregateRelBase m_groupByNode;
    private FilterRelBase m_havingNode;
    private SortRel m_orderByNode;
    private SortRel m_limitNode;
  };

  public static ASTNode convertOpTree(final RelNode relNode) {
    OptiqRelToHiveASTConverter converter = new OptiqRelToHiveASTConverter();
    OptiqRelTreeIntroduceDerivedTables.convertOpTree(relNode, null);
    return converter.convert(relNode, false);
  }

  public ASTNode convert(RelNode relNode, boolean subQuery) {
    ASTNode fromASTNode = null;
    ASTNode filterASTNode = null;
    ASTNode tmpDstASTNode = null;
    ASTNode selectASTNode = null;
    ASTNode gbASTNode = null;
    ASTNode havingASTNode = null;
    ASTNode obASTNode = null;
    ASTNode limitASTNode = null;

    OptiqNodesForHiveSelectStmt optiqNodesForSelectStmt = extractSelectStmtNodes(relNode);

    // Convert select stmt blocks to ASTNode
    fromASTNode = getFromNode(optiqNodesForSelectStmt.m_fromNode, optiqNodesForSelectStmt);
    setupColToTabMap(optiqNodesForSelectStmt);
    if (optiqNodesForSelectStmt.m_filterNode != null) {
    	      filterASTNode = getFilterNode(optiqNodesForSelectStmt.m_filterNode.getCondition(),
          optiqNodesForSelectStmt);
    }
    tmpDstASTNode = getTmpFileDestination();
    selectASTNode = getSelectNode(optiqNodesForSelectStmt.m_selectNode, optiqNodesForSelectStmt);
    if (optiqNodesForSelectStmt.m_groupByNode != null) {
      gbASTNode = getGBNode(optiqNodesForSelectStmt.m_groupByNode, optiqNodesForSelectStmt);
    }
    if (optiqNodesForSelectStmt.m_havingNode != null) {
      havingASTNode = getHavingNode(optiqNodesForSelectStmt.m_havingNode, optiqNodesForSelectStmt);
    }
    if (optiqNodesForSelectStmt.m_orderByNode != null) {
      obASTNode = getOBNode(optiqNodesForSelectStmt.m_orderByNode, optiqNodesForSelectStmt);
    }
    if (optiqNodesForSelectStmt.m_limitNode != null) {
      limitASTNode = getLimitNode(optiqNodesForSelectStmt.m_limitNode, optiqNodesForSelectStmt);
    }

    // Create Insert Node
    ASTNode insertASTNode = new ASTNode(new CommonToken(HiveParser.TOK_INSERT, "TOK_INSERT"));
    insertASTNode.addChild(tmpDstASTNode);
    insertASTNode.addChild(selectASTNode);
    if (filterASTNode != null) {
      insertASTNode.addChild(filterASTNode);
    }
    if (gbASTNode != null) {
      insertASTNode.addChild(gbASTNode);
    }
    if (havingASTNode != null) {
      insertASTNode.addChild(havingASTNode);
    }
    if (obASTNode != null) {
      insertASTNode.addChild(obASTNode);
    }
    if (limitASTNode != null) {
      insertASTNode.addChild(limitASTNode);
    }

    // Create Query Node
    ASTNode queryASTNode = new ASTNode(new CommonToken(HiveParser.TOK_QUERY, "TOK_QUERY"));
    queryASTNode.addChild(fromASTNode);
    queryASTNode.addChild(insertASTNode);

    if (subQuery) {
      ASTNode subQueryASTNode  = new ASTNode(new CommonToken(HiveParser.TOK_SUBQUERY, "TOK_SUBQUERY"));
      subQueryASTNode.addChild(queryASTNode);
      queryASTNode = subQueryASTNode;
    }

    return queryASTNode;
  }

  private void setupColToTabMap(OptiqNodesForHiveSelectStmt stateInfo) {
    Set<Entry<RelNode, Map<String, Pair<String, String>>>> opToColTabMapSet = stateInfo.m_opToColTabMap
        .entrySet();
    if (opToColTabMapSet.size() > 1) {
      throw new RuntimeException("FromNode schema Map found more than one entry");
    }

    stateInfo.m_colTabMap = opToColTabMapSet.iterator().next().getValue();
    stateInfo.m_opToColTabMap.clear();
  }

  private ASTNode getGBNode(AggregateRelBase groupByNode, OptiqNodesForHiveSelectStmt stateInfo) {
    // TODO Auto-generated method stub
    return null;
  }

  private ASTNode getSelectProjection(String originalColumnName, String columnAlias,
      OptiqNodesForHiveSelectStmt stateInfo) {
    Pair<String, String> colTabPair = stateInfo.m_colTabMap.get(originalColumnName);

    ASTNode selExpr = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
    // TODO handle expressions
    selExpr.addChild(getTableDotColumnName(colTabPair));
    selExpr.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, columnAlias)));

    return selExpr;
  }

  public static ASTNode getTableDotColumnName(Pair<String, String> colTabPair) {
    if (colTabPair == null) {
      throw new RuntimeException("Table.Column Pair can't be NULL");
    }

    ASTNode dotNode = new ASTNode(new CommonToken(HiveParser.DOT, "."));
    ASTNode tabColNode = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL,
        "TOK_TABLE_OR_COL"));
    tabColNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, colTabPair.getFirst())));
    dotNode.addChild(tabColNode);
    dotNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, colTabPair.getSecond())));

    return dotNode;
  }

	private ASTNode getSelectNode(SingleRel selectNode,
	    OptiqNodesForHiveSelectStmt stateInfo) {
		ASTNode selectASTNode = null;
		// List<RexNode> projectionsInRex = selectNode.getChildExps();
		RelDataType outputType = null;
		if (selectNode instanceof EnumerableCalcRel) {
			outputType = ((EnumerableCalcRel) selectNode).getProgram()
			    .getOutputRowType();
		} else {
			outputType = selectNode.getRowType();
		}
		List<ASTNode> projectionsInAST = new LinkedList<ASTNode>();

		if (selectNode.isDistinct()) {
			selectASTNode = new ASTNode(new CommonToken(HiveParser.TOK_SELECTDI,
			    "TOK_SELECTDI"));
		} else {
			selectASTNode = new ASTNode(new CommonToken(HiveParser.TOK_SELECT,
			    "TOK_SELECT"));
		}

		int i = 0;
		for (RexNode r : selectNode.getChildExps()) {
			ASTNode selectExpr = OptiqRelExprToHiveASTConverter.astExpr(
			    stateInfo.m_fromNode, r, stateInfo.m_colTabMap);
			String oAlias = outputType.getFieldNames().get(i);
			ASTNode selAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR,
			    "TOK_SELEXPR"));
			selAST.addChild(selectExpr);
			selAST.addChild((ASTNode) ParseDriver.adaptor.create(
			    HiveParser.Identifier, oAlias));
			projectionsInAST.add(selAST);
		}

		selectASTNode.addChildren(projectionsInAST);

		return selectASTNode;
	}

  private ASTNode getFilterNode(RexNode filterCond,
      OptiqNodesForHiveSelectStmt stateInfo) {
    // TODO: Include SubQuery
    ASTNode filterASTNode = new ASTNode(new CommonToken(HiveParser.TOK_WHERE, "TOK_WHERE"));
    ASTNode condnASTNode = OptiqRelExprToHiveASTConverter.astExpr(stateInfo.m_fromNode,
    		filterCond, stateInfo.m_colTabMap);
    filterASTNode.addChild(condnASTNode);

    return filterASTNode;
  }

  private ASTNode getOBNode(SortRel orderByNode, OptiqNodesForHiveSelectStmt stateInfo) {
    // TODO Auto-generated method stub
    return null;
  }

  private ASTNode getLimitNode(SortRel limitNode, OptiqNodesForHiveSelectStmt stateInfo) {
    // TODO Auto-generated method stub
    return null;
  }

  private ASTNode getHavingNode(FilterRelBase havingNode, OptiqNodesForHiveSelectStmt stateInfo) {
    // TODO
    return null;
  }

	private ASTNode getTableNode(TableAccessRelBase tableRel,
			OptiqNodesForHiveSelectStmt stateInfo) {
		RelOptHiveTable hTbl = (RelOptHiveTable) ((HiveTableScanRel) tableRel)
				.getTable();

		ASTNode tablRef = (ASTNode) ParseDriver.adaptor.create(
				HiveParser.TOK_TABREF, "TOK_TABREF");
		ASTNode tabNode = (ASTNode) ParseDriver.adaptor.create(
				HiveParser.TOK_TABNAME, "TOK_TABNAME");
		ParseDriver.adaptor.addChild(tabNode, ParseDriver.adaptor.create(
				HiveParser.Identifier, hTbl.getHiveTableMD().getDbName()));
		ParseDriver.adaptor.addChild(tabNode, ParseDriver.adaptor.create(
				HiveParser.Identifier, hTbl.getHiveTableMD().getTableName()));
		ParseDriver.adaptor.addChild(tablRef, tabNode);
		ParseDriver.adaptor.addChild(
				tablRef,
				ParseDriver.adaptor.create(HiveParser.Identifier,
						hTbl.getName()));

		assemblecolToTabMap(tableRel, stateInfo);

		return tablRef;
	}

  private ASTNode getJoinchildNode(RelNode joinChild, OptiqNodesForHiveSelectStmt stateInfo) {
    ASTNode joinChildASTNode = null;

    if (joinChild instanceof TableAccessRelBase) {
      joinChildASTNode = getTableNode((TableAccessRelBase) joinChild, stateInfo);
    } else if (joinChild instanceof EnumerableCalcRel || joinChild instanceof HiveProjectRel) {
      joinChildASTNode = convert(joinChild, true);
      String tabName = getUniqueDerivedTabName(null);
      assemblecolToTabMap((SingleRel) joinChild, stateInfo, tabName);
      joinChildASTNode.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, tabName)));
    } else if (joinChild instanceof JoinRelBase) {
      joinChildASTNode = getJoinToken((JoinRelBase) joinChild, stateInfo);
    } else {
      throw new RuntimeException("Unsupported RelNode Type below Join");
    }

    return joinChildASTNode;
  }

  private ASTNode getJoinToken(JoinRelBase jb, OptiqNodesForHiveSelectStmt stateInfo) {
    ASTNode joinToken = null;// new ASTNode(new CommonToken(HiveParser.TOK_FROM, "TOK_FROM"));
    ASTNode leftASTNode = getJoinchildNode(jb.getLeft(), stateInfo);
    ASTNode rightASTNode = getJoinchildNode(jb.getRight(), stateInfo);
    assemblecolToTabMap(jb, stateInfo);
    ASTNode onclauseASTNode = OptiqRelExprToHiveASTConverter.astExpr(jb, jb.getCondition(),
        stateInfo.m_opToColTabMap.get(jb));

    int joinType = -1;
    String joinTypeTok = null;
    switch (jb.getJoinType()) {
    case INNER: {
      joinType = HiveParser.TOK_JOIN;
      joinTypeTok = "TOK_JOIN";
    }
      break;
    case LEFT: {
      joinType = HiveParser.TOK_LEFTOUTERJOIN;
      joinTypeTok = "TOK_LEFTOUTERJOIN";
    }
      break;
    case RIGHT: {
      joinType = HiveParser.TOK_RIGHTOUTERJOIN;
      joinTypeTok = "TOK_RIGHTOUTERJOIN";
    }
      break;
    case FULL: {
      joinType = HiveParser.TOK_FULLOUTERJOIN;
      joinTypeTok = "TOK_FULLOUTERJOIN";
    }
      break;
    default:
      throw new RuntimeException("Unexpected Join Type");
    }
    joinToken = new ASTNode(new CommonToken(joinType, joinTypeTok));

    joinToken.addChild(leftASTNode);
    joinToken.addChild(rightASTNode);
    joinToken.addChild(onclauseASTNode);

    return joinToken;
  }

  private String getUniqueDerivedTabName(HashSet<String> uniqueTabNames) {
    String tabName = null;
    boolean notDone = true;
    int retryCount = 0;
    int retrylimt = 100;

    // TODO: retry & random counter value is a hack; needs to revisit this.
    do {
      tabName = new String("$hdt$_" + m_tabCounter.getAndIncrement());
      if (uniqueTabNames != null && uniqueTabNames.contains(tabName)) {
        m_tabCounter.set(m_rand.nextInt());
        tabName = null;
      } else {
        notDone = false;
      }
      retryCount++;
    } while (notDone && retryCount < retrylimt);

    if (tabName == null) {
      throw new RuntimeException("Unable to generate unique table name");
    }

    return tabName;
  }

  private void assemblecolToTabMap(SingleRel fromNode,
      OptiqNodesForHiveSelectStmt stateInfo, String tabName) {
    HashMap<String, Pair<String, String>> colToTabMap = new HashMap<String, Pair<String, String>>();

    for (RelDataTypeField field : fromNode.getRowType().getFieldList()) {
      colToTabMap.put(field.getName(), new Pair<String, String>(tabName, field.getName()));
    }

    stateInfo.m_opToColTabMap.put(fromNode, colToTabMap);
    // TODO: is this really needed (since each select would have new OptiqNodesForHiveSelectStmt,
    // the stmt below looks like noop
    stateInfo.m_opToColTabMap.remove(fromNode.getInput(0));
  }

  private void assemblecolToTabMap(TableAccessRelBase fromNode,
      OptiqNodesForHiveSelectStmt stateInfo) {
    HashMap<String, Pair<String, String>> colToTabMap = new HashMap<String, Pair<String, String>>();
    String tabName = fromNode.getTable().getQualifiedName().get(0);

    for (RelDataTypeField field : fromNode.getRowType().getFieldList()) {
      colToTabMap.put(field.getName(), new Pair<String, String>(tabName, field.getName()));
    }

    stateInfo.m_opToColTabMap.put(fromNode, colToTabMap);
  }


  private void assemblecolToTabMap(JoinRelBase fromNode, OptiqNodesForHiveSelectStmt stateInfo) {
    HashMap<String, Pair<String, String>> colToTabMap = new HashMap<String, Pair<String, String>>();
    int leftSchemaSize = fromNode.getLeft().getRowType().getFieldCount();
    RelNode inputRel;
    int colPosInInputRel;
    Map<String, Pair<String, String>> leftInputMap = stateInfo.m_opToColTabMap.get(fromNode
        .getLeft());
    Map<String, Pair<String, String>> rightInputMap = stateInfo.m_opToColTabMap.get(fromNode
        .getRight());
    Map<String, Pair<String, String>> inputMap;

    for (RelDataTypeField field : fromNode.getRowType().getFieldList()) {
      if (field.getIndex() < leftSchemaSize) {
        inputRel = fromNode.getLeft();
        colPosInInputRel = field.getIndex();
        inputMap = leftInputMap;
      }
      else {
        inputRel = fromNode.getRight();
        colPosInInputRel = field.getIndex() - leftSchemaSize;
        inputMap = rightInputMap;
      }

      colToTabMap.put(field.getName(),
          inputMap.get(inputRel.getRowType().getFieldNames().get(colPosInInputRel)));
    }

    stateInfo.m_opToColTabMap.remove(((JoinRelBase) fromNode).getLeft());
    stateInfo.m_opToColTabMap.remove(((JoinRelBase) fromNode).getRight());

    stateInfo.m_opToColTabMap.put(fromNode, colToTabMap);
  }

  private ASTNode getFromNode(RelNode fromNode, OptiqNodesForHiveSelectStmt stateInfo) {
    ASTNode fromASTNode = new ASTNode(new CommonToken(HiveParser.TOK_FROM, "TOK_FROM"));
    ASTNode childNodeOfFrom = null;

    if (fromNode instanceof EnumerableCalcRel || fromNode instanceof ProjectRelBase ) {
      childNodeOfFrom = convert(fromNode, true);
      String tabName = getUniqueDerivedTabName(null);
      assemblecolToTabMap((SingleRel) fromNode, stateInfo, tabName);
      childNodeOfFrom.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, tabName)));
    } else if (fromNode instanceof JoinRelBase) {
      childNodeOfFrom = getJoinToken((JoinRelBase) fromNode, stateInfo);
    } else if (fromNode instanceof TableAccessRelBase) {
      // TODO introduce table alias if needed to handle self join
      childNodeOfFrom = getTableNode((TableAccessRelBase) fromNode, stateInfo);
    } else {
      throw new RuntimeException("Unexpected Node found for from node");
    }

    fromASTNode.addChild(childNodeOfFrom);

    return fromASTNode;
  }

  private ASTNode getTmpFileDestination()
  {
    ASTNode tmpASTNode = new ASTNode(new CommonToken(HiveParser.TOK_TMP_FILE, "TOK_TMP_FILE"));
    ASTNode dirASTNode = new ASTNode(new CommonToken(HiveParser.TOK_DIR, "TOK_DIR"));
    dirASTNode.addChild(tmpASTNode);
    ASTNode destASTNode = new ASTNode(
        new CommonToken(HiveParser.TOK_DESTINATION, "TOK_DESTINATION"));
    destASTNode.addChild(dirASTNode);

    return destASTNode;
  }

  private OptiqNodesForHiveSelectStmt extractSelectStmtNodes(RelNode relNode) {
    boolean doneExtractingNodes = false;
    OptiqNodesForHiveSelectStmt hiveSelectStmt = new OptiqNodesForHiveSelectStmt();

    hiveSelectStmt.m_selectNode = (SingleRel) relNode;
    relNode = relNode.getInput(0);
    while (!doneExtractingNodes) {
      if (relNode instanceof SortRel) {
        if (((SortRel) relNode).fetch != null) {
          hiveSelectStmt.m_limitNode = (SortRel) relNode;
        }
        if (!((SortRel) relNode).getCollation().getFieldCollations().isEmpty())
        {
          hiveSelectStmt.m_orderByNode = (SortRel) relNode;
        }
      } else if (relNode instanceof FilterRelBase) {
        if (relNode.getInput(0) instanceof AggregateRel) {
          hiveSelectStmt.m_havingNode = (FilterRelBase) relNode;
        } else {
        	hiveSelectStmt.m_filterNode = (FilterRelBase) relNode;
        }
      } else if (relNode instanceof AggregateRelBase) {
        hiveSelectStmt.m_groupByNode = (AggregateRelBase) relNode;
      } else if (relNode instanceof JoinRelBase || relNode instanceof HiveProjectRel
          || relNode instanceof TableAccessRelBase) {
        hiveSelectStmt.m_fromNode = relNode;
        doneExtractingNodes = true;
      }

      if (!doneExtractingNodes) {
        relNode = relNode.getInput(0);
      }
    }

    return hiveSelectStmt;
  }

}
