package org.apache.hadoop.hive.ql.optimizer.optiq.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.Pair;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveAggregateRel;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.WindowRelBase.RexWinAggCall;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCorrelVariable;
import org.eigenbase.rex.RexDynamicParam;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.eigenbase.rex.RexRangeRef;
import org.eigenbase.util.NlsString;

public class OptiqStatsUtil {

    public static long computeDegreeOfParallelization(HiveRel rel) {
        return (long) ((rel.getAvgTupleSize() * rel.getRows()) / (64 * 1024));
    }

    /*** Column Stats ***/
    // TODO: colStats should be kept in cache as long as they are valid. In this
    // scheme if a rule that could invalidate stats has fired and resulted in
    // tree mutation then the stats of all nodes above should be invalidated.

    public static List<HiveColStat> computeProjectRelColStat(
            HiveProjectRel projectRel, List<Integer> projIndxLst) {
        List<Integer> vColIndxs = projectRel.getVirtualCols();

        if (vColIndxs != null && !vColIndxs.isEmpty()) {
            List<Integer> colsFromChild = new ArrayList<Integer>(projIndxLst);
            List<HiveColStat> vColStats = new LinkedList<HiveColStat>();
            List<HiveColStat> colStatsFrmChild = new LinkedList<HiveColStat>();
            List<HiveColStat> mergedColStats = new LinkedList<HiveColStat>();
            List<RexNode> exps = projectRel.getChildExps();

            // TODO: Would this retain the order of the list.
            colsFromChild.removeAll(vColIndxs);
            colStatsFrmChild.addAll(OptiqUtil.getNonSubsetRelNode(
                    projectRel.getChild()).getColStat(colsFromChild));
            for (Integer vcIndx : vColIndxs) {
                vColStats.add(getColStatForVC(exps.get(vcIndx),
                        OptiqUtil.getNonSubsetRelNode(projectRel.getChild())));
            }

            mergedColStats.addAll(colStatsFrmChild);
            for (int i = 0; i < vColIndxs.size(); i++) {
                Integer vColIndx = vColIndxs.get(i);
                if (vColIndx > (mergedColStats.size() - 1))
                    vColStats.add(vColStats.get(i));
                else
                    mergedColStats.add(vColIndx, vColStats.get(i));
            }

            return mergedColStats;
        } else {
            return OptiqUtil.getNonSubsetRelNode(projectRel.getChild())
                    .getColStat(projIndxLst);
        }
    }

    private static HiveColStat getColStatForVC(RexNode rexNode, HiveRel child) {
        Pair<Double, Long> szNDVPair = getAvgSizeAndNDV(rexNode, child, true);
        return (new HiveColStat(szNDVPair.getFirst(), szNDVPair.getSecond()));
    }

    // TODO: clean this up
    public static List<HiveColStat> computeAggregateRelColStat(
            HiveAggregateRel aggRel, List<Integer> projIndxLst) {
        int noOfGroupingCols = aggRel.getGroupCount();
        List<AggregateCall> aggCallLst = aggRel.getAggCallList();
        List<RelDataTypeField> fieldLst = aggRel.getRowType().getFieldList();

        List<Integer> childColStatRqd = new LinkedList<Integer>();
        HashMap<Integer, Integer> childProjIndxAlreadyUsed = new HashMap<Integer, Integer>();
        HashMap<Integer, List<Integer>> aggProjIndxToaggRelColStatIndxMap = new HashMap<Integer, List<Integer>>();
        Integer tmpInteger;
        Integer tmpInteger1;
        
        // 1. Assemble indexes from child for grouping cols
        List<Integer> tmpAggCallArgLst;
        List<Integer> aggRelColStatIndxLst;

        for (int i = 0; i < projIndxLst.size(); i++) {
            Integer projIndx = projIndxLst.get(i);
            if (projIndx < noOfGroupingCols) {
                tmpInteger = fieldLst.get(i).getIndex();
                tmpInteger1 = childProjIndxAlreadyUsed.get(tmpInteger);

                if (tmpInteger1 == null) {
                    childProjIndxAlreadyUsed.put(tmpInteger, i);
                    aggRelColStatIndxLst = new LinkedList<Integer>();
                    aggRelColStatIndxLst.add(childColStatRqd.size());
                    aggProjIndxToaggRelColStatIndxMap.put(i,
                            aggRelColStatIndxLst);
                    childColStatRqd.add(tmpInteger);
                } else {
                    aggRelColStatIndxLst = new LinkedList<Integer>();
                    aggRelColStatIndxLst.add(tmpInteger1);
                    aggProjIndxToaggRelColStatIndxMap.put(i,
                            aggRelColStatIndxLst);
                }
            } else {
                // 2. Collect indexes for cols that are Agg Func

                // Cols for Agg Func comes after grouping cols
                tmpAggCallArgLst = aggCallLst.get(projIndx - noOfGroupingCols)
                        .getArgList();

                // Walk through all of the args to Agg Func and add their
                // corresponding child index to the list and Map
                if (tmpAggCallArgLst != null) {
                    aggRelColStatIndxLst = new LinkedList<Integer>();

                    for (Integer argChildIndx : tmpAggCallArgLst) {
                        tmpInteger = childProjIndxAlreadyUsed.get(argChildIndx);
                        if (tmpInteger == null) {
                            tmpInteger = childColStatRqd.size();
                            aggRelColStatIndxLst.add(tmpInteger);
                            childColStatRqd.add(argChildIndx);
                            childProjIndxAlreadyUsed.put(argChildIndx,
                                    tmpInteger);
                        } else {
                            aggRelColStatIndxLst.add(tmpInteger);
                        }
                    }
                    aggProjIndxToaggRelColStatIndxMap.put(i,
                            aggRelColStatIndxLst);
                }
            }
        }

        List<HiveColStat> colStatFromChild = OptiqUtil.getNonSubsetRelNode(
                aggRel.getChild()).getColStat(childColStatRqd);
 
        HiveColStat tmpColStat;
        double tmpMaxAvgSize;
        long tmpMaxNDV;
        List<HiveColStat> colStatToRet = new LinkedList<HiveColStat>();

        for (int i = 0; i < childColStatRqd.size(); i++) {
            aggRelColStatIndxLst = aggProjIndxToaggRelColStatIndxMap
                    .get(i);
            tmpMaxAvgSize = 0;
            tmpMaxNDV = 0;
            for (Integer j : aggRelColStatIndxLst) {
                tmpColStat = colStatFromChild.get(j);
                if (tmpColStat.getAvgSz() > tmpMaxAvgSize)
                    tmpMaxAvgSize = tmpColStat.getAvgSz();
                if (tmpColStat.getNDV() > tmpMaxNDV)
                    tmpMaxNDV = tmpColStat.getNDV();
            }
            colStatToRet.add(new HiveColStat(tmpMaxAvgSize, tmpMaxNDV));
        }

        return colStatToRet;
    }

    // TODO: clean this up
    /*
     * public static List<HiveColStat> computeAggregateRelColStat(
     * HiveAggregateRel aggRel, List<Integer> projIndxLst) { int
     * noOfGroupingCols = aggRel.getGroupCount(); List<AggregateCall> aggCallLst
     * = aggRel.getAggCallList(); int noOfAggCols = (aggCallLst != null) ?
     * aggRel.getAggCallList().size() : 0;
     * 
     * List<RelDataTypeField> fieldLst = aggRel.getRowType().getFieldList();
     * 
     * HashMap<Integer, Integer> childToParentProjIndxMap = new HashMap<Integer,
     * Integer>(); HashMap<Integer, List<Integer>>
     * aggProjIndxToaggRelColStatIndxMap = new HashMap<Integer,
     * List<Integer>>(); List<Integer> childProjIndxLst = new
     * LinkedList<Integer>();
     * 
     * Integer tmpInteger;
     * 
     * int i = 0; for (Integer projIndx : projIndxLst) { if (projIndx <
     * noOfGroupingCols) { tmpInteger = fieldLst.get(i).getIndex();
     * childToParentProjIndxMap.put(tmpInteger, i);
     * childProjIndxLst.add(tmpInteger); } }
     * 
     * for (int i = 0; i < noOfGroupingCols; i++) { tmpInteger =
     * fieldLst.get(i).getIndex(); childToParentProjIndxMap.put(tmpInteger, i);
     * childProjIndxLst.add(tmpInteger); }
     * 
     * if (noOfAggCols > 0) { List<Integer> tmpAggCallArgLst; List<Integer>
     * aggRelColStatIndxLst;
     * 
     * for (int i = noOfGroupingCols; i < (noOfGroupingCols + noOfAggCols); i++)
     * { tmpAggCallArgLst = aggCallLst.get(i - noOfGroupingCols).getArgList();
     * 
     * if (tmpAggCallArgLst != null) { aggRelColStatIndxLst = new
     * LinkedList<Integer>();
     * 
     * for (Integer argChildIndx : tmpAggCallArgLst) { tmpInteger =
     * childToParentProjIndxMap.get(argChildIndx); if (tmpInteger == null) {
     * tmpInteger = childProjIndxLst.size();
     * aggRelColStatIndxLst.add(tmpInteger); childProjIndxLst.add(argChildIndx);
     * childToParentProjIndxMap.put(argChildIndx, tmpInteger); } else {
     * aggRelColStatIndxLst .add(tmpInteger); } }
     * aggProjIndxToaggRelColStatIndxMap.put(i, aggRelColStatIndxLst); } } }
     * 
     * List<HiveColStat> colStatFromChild = OptiqUtil.getNonSubsetRelNode(
     * aggRel.getChild()).getColStat(childProjIndxLst); List<HiveColStat>
     * colStatToRet = new LinkedList<HiveColStat>(colStatFromChild.subList(0,
     * noOfGroupingCols));
     * 
     * if (noOfAggCols > 0) { HiveColStat tmpColStat; double tmpMaxAvgSize; long
     * tmpMaxNDV; List<Integer> aggRelColStatIndxLst = new
     * LinkedList<Integer>();
     * 
     * for (int i = noOfGroupingCols; i < (noOfGroupingCols + noOfAggCols); i++)
     * { aggRelColStatIndxLst = aggProjIndxToaggRelColStatIndxMap.get(i);
     * tmpMaxAvgSize = 0; tmpMaxNDV = 0; for (Integer j : aggRelColStatIndxLst)
     * { tmpColStat = colStatFromChild.get(j); if (tmpColStat.getAvgSz() >
     * tmpMaxAvgSize) tmpMaxAvgSize = tmpColStat.getAvgSz(); if
     * (tmpColStat.getNDV() > tmpMaxNDV) tmpMaxNDV = tmpColStat.getNDV(); }
     * colStatToRet.add(new HiveColStat(tmpMaxAvgSize, tmpMaxNDV)); } }
     * 
     * return colStatToRet; }
     */

    public static List<HiveColStat> getJoinRelColStat(HiveJoinRel j,
            List<Integer> projIndxLst) {
        List<HiveColStat> colStats = new LinkedList<HiveColStat>();
        Pair<List<Integer>, List<Integer>> separatedProjIndxs = OptiqUtil
                .translateProjIndxToChild(j, projIndxLst);
        List<Integer> projIndxsFromLeft = separatedProjIndxs.getFirst();
        List<Integer> projIndxsFromRight = separatedProjIndxs.getSecond();
        colStats.addAll(OptiqUtil.getNonSubsetRelNode(j.getLeft()).getColStat(
                projIndxsFromLeft));
        colStats.addAll(OptiqUtil.getNonSubsetRelNode(j.getRight()).getColStat(
                projIndxsFromRight));

        return colStats;
    }

    // TODO: current implementation assumes the position of child columns will
    // be maintained as it is in the Union OP. This may not be true. Based on
    // type match Union may select different projections from child for a
    // projection index.
    public static List<HiveColStat> computeUnionRelColStat(
            List<RelNode> inputs, List<Integer> projIndxLst) {
        HiveRel hiveChildNode;
        List<Double> avgColSzLst = new LinkedList<Double>();
        List<Long> ndvLst = new LinkedList<Long>();
        List<HiveColStat> colStatLst = new LinkedList<HiveColStat>();

        List<HiveColStat> hcsLst;
        hcsLst = ((HiveRel) (OptiqUtil.getNonSubsetRelNode(inputs.get(0))))
                .getColStat(projIndxLst);
        for (int i = 0; i < hcsLst.size(); i++) {
            (avgColSzLst).add(i, hcsLst.get(i).getAvgSz());
            (ndvLst).add(i, hcsLst.get(i).getNDV());
        }

        Double tmpDouble;
        Long tmpLong;
        for (RelNode child : inputs) {
            hiveChildNode = OptiqUtil.getNonSubsetRelNode(child);
            hcsLst = hiveChildNode.getColStat(projIndxLst);
            for (int i = 0; i < hcsLst.size(); i++) {
                tmpDouble = avgColSzLst.get(i);
                avgColSzLst.set(i, tmpDouble + hcsLst.get(i).getAvgSz());
                tmpLong = ndvLst.get(i);
                ndvLst.set(i, Math.max(tmpLong, hcsLst.get(i).getNDV()));
            }
        }

        int noInputs = inputs.size();
        for (int i = 0; i < avgColSzLst.size(); i++) {
            colStatLst.add(new HiveColStat((avgColSzLst.get(i) / noInputs),
                    ndvLst.get(i)));
        }

        return colStatLst;
    }

    public static List<HiveColStat> computeTableRelColStat(
            RelOptHiveTable hiveTbl, List<String> colNamesLst) {
        List<HiveColStat> hiveColStats = new LinkedList<HiveColStat>();

        Statistics stats = hiveTbl.getColumnStats(colNamesLst);
        if (stats.getColumnStats().size() != colNamesLst.size())
            throw new RuntimeException("Incomplete Col stats");
        
        for (ColStatistics colStat : stats.getColumnStats()) {
            hiveColStats.add(new HiveColStat((long) colStat.getAvgColLen(),
                    colStat.getCountDistint()));
        }

        return hiveColStats;
    }

    public static HiveColStat computeColStat(HiveRel rel, Integer projIndx) {
        HiveColStat colStat = null;
        List<HiveColStat> colStatLst;
        List<Integer> projIndxLst = new ArrayList<Integer>();

        projIndxLst.add(projIndx);
        colStatLst = rel.getColStat(projIndxLst);
        if (colStatLst != null)
            colStat = colStatLst.get(0);

        return colStat;
    }

    /*** Memory Computation ***/
    public static double computeAvgTupleSize(List<HiveColStat> colStatLst) {
        double avgTupleSz = 0;
        for (HiveColStat cs : colStatLst) {
            avgTupleSz += cs.getAvgSz();
        }
        return avgTupleSz;
    }

    public static double computeProjectRelAvgTupleSize(HiveProjectRel projectRel) {
        HiveRel child = OptiqUtil.getNonSubsetRelNode(projectRel.getChild());
        List<RexNode> exps = projectRel.getChildExps();
        double virtualColSize = OptiqStatsUtil.getAvgSize(exps,
                (HiveRel) child, true);
        double avgTupleSize = ((HiveRel) child).getAvgTupleSize()
                + virtualColSize;

        return (virtualColSize + avgTupleSize);
    }

    public static double computeUnionRelAvgTupleSize(List<RelNode> inputs) {
        HiveRel hiveChildNode;
        double totalAvgSz = 0.0;
        for (RelNode child : inputs) {
            hiveChildNode = OptiqUtil.getNonSubsetRelNode(child);
            totalAvgSz += hiveChildNode.getAvgTupleSize();
        }
        return (totalAvgSz / inputs.size());
    }

    public static double getAvgSize(List<RexNode> exps, HiveRel child,
            boolean ignoreInputRef) {
        double avgProjSz = 0;

        for (RexNode expNode : exps) {
            avgProjSz += getAvgSize(expNode, child, true);
        }

        return avgProjSz;
    }

    private static Pair<Double, Long> getAvgSizeAndNDV(RexNode exp,
            HiveRel child, boolean ignoreInputRef) {
        HiveColStat tmp;
        Double avgSize = 0.0;
        Long ndv = (long) 0;

        if (exp instanceof RexInputRef && !ignoreInputRef) {
            tmp = child.getColStat(((RexInputRef) exp).getIndex());
            avgSize = tmp.getAvgSz();
            ndv = tmp.getNDV();
        } else if (exp instanceof RexCorrelVariable) {
            // DO Nothing, return size of 0
        } else if (exp instanceof RexDynamicParam) {
            // TODO: is the index valid? does it point to child schema element?
            tmp = child.getColStat(((RexDynamicParam) exp).getIndex());
            avgSize = tmp.getAvgSz();
            ndv = tmp.getNDV();
        } else if (exp instanceof RexLocalRef) {
            // TODO: is the index valid? does it point to child schema element?
            tmp = child.getColStat(((RexLocalRef) exp).getIndex());
            avgSize = tmp.getAvgSz();
            ndv = tmp.getNDV();
        } else if (exp instanceof RexOver) {
            Pair<Double, Long> szNDVPair = getMaxAvgSizeNDV(
                    ((RexOver) exp).getOperands(), child, ignoreInputRef);
            avgSize = szNDVPair.getFirst();
            ndv = szNDVPair.getSecond();
        } else if (exp instanceof RexWinAggCall) {
            Pair<Double, Long> szNDVPair = getMaxAvgSizeNDV(
                    ((RexWinAggCall) exp).getOperands(), child, ignoreInputRef);
            avgSize = szNDVPair.getFirst();
            ndv = szNDVPair.getSecond();
        } else if (exp instanceof RexFieldAccess) {
            // TODO: How do we know if RexFieldAccess is referring to its child
            // node's schema
            tmp = child
                    .getColStat(((RexFieldAccess) exp).getField().getIndex());
            avgSize = tmp.getAvgSz();
            ndv = tmp.getNDV();
        } else if (exp instanceof RexLiteral) {
            avgSize = (double) getSizeOfLiteral((RexLiteral) exp);
            ndv = (long) 1;
        } else if (exp instanceof RexRangeRef) {

        }

        return (new Pair<Double, Long>(avgSize, ndv));
    }

    private static Pair<Double, Long> getMaxAvgSizeNDV(List<RexNode> expLst,
            HiveRel child, boolean ignoreInputRef) {
        Double avgSize = (double) 0;
        Long ndv = (long) 0;
        Pair<Double, Long> tmpPair;

        for (RexNode exp : expLst) {
            tmpPair = getAvgSizeAndNDV(exp, child, ignoreInputRef);
            if (tmpPair.getFirst() > avgSize) {
                avgSize = tmpPair.getFirst();
            }
            if (tmpPair.getSecond() > ndv) {
                ndv = tmpPair.getSecond();
            }
        }

        return (new Pair<Double, Long>(avgSize, ndv));
    }

    private static double getAvgSize(RexNode exp, HiveRel child,
            boolean ignoreInputRef) {
        double avgSize = 0;

        if (exp instanceof RexInputRef && !ignoreInputRef) {
            avgSize = child.getColStat(((RexInputRef) exp).getIndex())
                    .getAvgSz();
        } else if (exp instanceof RexCorrelVariable) {
            // DO Nothing, return size of 0
        } else if (exp instanceof RexDynamicParam) {
            // TODO: is the index valid? does it point to child schema element?
            avgSize = child.getColStat(((RexDynamicParam) exp).getIndex())
                    .getAvgSz();
        } else if (exp instanceof RexLocalRef) {
            // TODO: is the index valid? does it point to child schema element?
            avgSize = child.getColStat(((RexLocalRef) exp).getIndex())
                    .getAvgSz();
        } else if (exp instanceof RexOver) {
            avgSize = getMaxAvgSize(((RexOver) exp).getOperands(), child,
                    ignoreInputRef);
        } else if (exp instanceof RexWinAggCall) {
            avgSize = getMaxAvgSize(((RexWinAggCall) exp).getOperands(), child,
                    ignoreInputRef);
        } else if (exp instanceof RexFieldAccess) {
            // TODO: How do we know if RexFieldAccess is referring to its child
            // node's schema
            avgSize = child.getColStat(
                    ((RexFieldAccess) exp).getField().getIndex()).getAvgSz();
        } else if (exp instanceof RexLiteral) {
            avgSize = getSizeOfLiteral((RexLiteral) exp);
        } else if (exp instanceof RexRangeRef) {

        }

        return avgSize;
    }

    // TODO: 1) size should be the size of the value in Hive.
    // Size calculations for literals is complicated due to sharing of immutable
    // in JVM
    // 2) Evaluate the sizes, may not be correct?
    private static long getSizeOfLiteral(RexLiteral literal) {
        long sz = 0;

        switch (literal.getTypeName()) {
        case ANY:
            break;
        case ARRAY:
            break;
        case DOUBLE:
        case DECIMAL:
            sz = 8;
            break;
        case FLOAT:
        case REAL:
            sz = 4;
            break;
        case TINYINT:
            sz = 1;
            break;
        case SMALLINT:
            sz = 2;
            break;
        case INTEGER:
            sz = 4;
            break;
        case BIGINT:
            sz = 8;
            break;
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
        case DATE:
        case TIME:
        case TIMESTAMP:
            sz = 12;
            break;

        case BOOLEAN:
            sz = 1;
            break;

        case CHAR:
        case VARCHAR:
            if (literal.getValue() instanceof NlsString) {
                sz = (((NlsString) (literal.getValue())).getValue().length() * 2);
            }
            break;

        case BINARY:
            break;
        case VARBINARY:
            break;

        case COLUMN_LIST:
            break;
        case CURSOR:
            break;
        case DISTINCT:
            break;
        case MAP:
            break;
        case MULTISET:
            break;
        case NULL:
            break;
        case OTHER:
            break;
        case ROW:
            break;
        case STRUCTURED:
            break;
        case SYMBOL:
            break;
        default:
            break;
        }

        return sz;
    }

    private static double getMaxAvgSize(List<RexNode> expLst, HiveRel child,
            boolean ignoreInputRef) {
        double avgSize = 0;
        double tmpSize = 0;
        for (RexNode exp : expLst) {
            tmpSize = getAvgSize(exp, child, ignoreInputRef);
            if (tmpSize > avgSize) {
                avgSize = tmpSize;
            }
        }

        return avgSize;
    }

    /*
     * private static List<Double> getAvgSize(List<RexInputRef> refLst, HiveRel
     * child) { List<Integer> projIdxLst = new LinkedList<Integer>();
     * List<Double> projAvgSizeLst;
     * 
     * for (RexInputRef inRef : refLst) { projIdxLst.add(inRef.getIndex()); }
     * projAvgSizeLst = child.getColumnAvgSize(projIdxLst);
     * 
     * return projAvgSizeLst; }
     * 
     * private static Double getAvgSize(RexNode func, HiveRel child) {
     * List<Integer> projIdxLst = new LinkedList<Integer>(); List<Double>
     * projAvgSizeLst;
     * 
     * for (RexInputRef inRef : refLst) { projIdxLst.add(inRef.getIndex()); }
     * projAvgSizeLst = child.getColumnAvgSize(projIdxLst);
     * 
     * return projAvgSizeLst; }
     */
}
