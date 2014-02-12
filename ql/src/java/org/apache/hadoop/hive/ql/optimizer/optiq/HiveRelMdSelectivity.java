package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.optimizer.optiq.HiveOptiqJoinUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.HiveOptiqJoinUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.metadata.RelMdSelectivity;
import org.eigenbase.rel.metadata.RelMdUtil;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

import com.google.common.collect.ImmutableMap;

public class HiveRelMdSelectivity extends RelMdSelectivity {

  public Double getSelectivity(HiveTableScanRel t, RexNode predicate) {
    if (predicate != null) {
      FilterSelectivityEstimator filterSelEstmator = new FilterSelectivityEstimator(t);
      return filterSelEstmator.estimateSelectivity(predicate);
    }

    return 1.0;
  }

  public Double getSelectivity(HiveJoinRel j, RexNode predicate) {
    if (j.getJoinType().equals(JoinRelType.INNER)) {
      return computeInnerJoinSelectivity(j, predicate);
    }
    return 1.0;
  }

  /**
   * Compute Inner Join selectivity.
   * <p>
   * Inner Join Selectivity Logic:<br>
   * 1. Decompose Join condition to conjunctive elements.<br>
   * 2. Walk through conjunctive elements finding NDV.<br>
   * 3. Join Selectivity = 1/(NDV of (conjuctive element1))*(NDV of (conjuctive
   * element1)).<br>
   * 4. NDV of conjunctive element = max NDV (of args from LHS expr, RHS Expr)
   * 
   * @param j
   * @return double value will be within 0 and 1.
   * 
   *         TODO: 1. handle join conditions like "(r1.x=r2.x) and (r1.x=r2.y)"
   *         better
   */
  private Double computeInnerJoinSelectivity(HiveJoinRel j, RexNode predicate) {
    double joinSelectivity = 1;
    RexNode combinedPredicate = getCombinedPredicateForJoin(j, predicate);
    JoinPredicateInfo jpi = JoinPredicateInfo.constructJoinPredicateInfo(j, combinedPredicate);
    ImmutableMap.Builder<Integer, Double> colStatMapBuilder = ImmutableMap.builder();
    ImmutableMap<Integer, Double> colStatMap;
    int rightOffSet = j.getLeft().getRowType().getFieldCount();

    // 1. Update Col Stats Map with col stats for columns from left side of
    // Join which are part of join keys
    for (Integer ljk : jpi.getProjsFromLeftPartOfJoinKeysInChildSchema()) {
      colStatMapBuilder.put(ljk, HiveRelMdDistinctRowCount.getDistinctRowCount(j.getLeft(), ljk));
    }

    // 2. Update Col Stats Map with col stats for columns from right side of
    // Join which are part of join keys
    for (Integer rjk : jpi.getProjsFromRightPartOfJoinKeysInChildSchema()) {
      colStatMapBuilder.put(rjk + rightOffSet,
          HiveRelMdDistinctRowCount.getDistinctRowCount(j.getRight(), rjk));
    }

    colStatMap = colStatMapBuilder.build();

    // 3. Walk through the Join Condition Building Selectivity
    // Join Selectivity for inner join with Join Condition
    // "(left.expr1 = right.expr1) && (left.expr2 = right.expr2)"
    // = 1/(max(NDV(left.Expr1), NDV(right.Expr1)) * max(NDV(left.Expr2),
    // NDV(right.Expr2)))
    // NDV(expr) = max(NDV( expr args))
    for (JoinLeafPredicateInfo jlpi : jpi.getEquiJoinPredicateElements()) {
      joinSelectivity *= (1 / (getMaxNDVForJoinSelectivity(jlpi, colStatMap)));
    }

    return joinSelectivity;
  }

  private RexNode getCombinedPredicateForJoin(HiveJoinRel j, RexNode additionalPredicate) {
    RexNode minusPred = RelMdUtil.minusPreds(j.getCluster().getRexBuilder(), additionalPredicate,
        j.getCondition());

    if (minusPred != null) {
      List<RexNode> minusList = new ArrayList<RexNode>();
      minusList.add(j.getCondition());
      minusList.add(minusPred);

      return RexUtil.composeConjunction(j.getCluster().getRexBuilder(), minusList, true);
    }

    return j.getCondition();
  }

  /**
   * Compute Max NDV to determine Join Selectivity.
   * 
   * @param jlpi
   * @param colStatMap
   *          Immutable Map of Projection Index (in Join Schema) to Column Stat
   * @param rightProjOffSet
   * @return
   */
  private static Double getMaxNDVForJoinSelectivity(JoinLeafPredicateInfo jlpi,
      ImmutableMap<Integer, Double> colStatMap) {
    Double maxNDVSoFar = 1.0;

    maxNDVSoFar = getMaxNDVFromProjections(colStatMap,
        jlpi.getProjsFromLeftPartOfJoinKeysInJoinSchema(), maxNDVSoFar);
    maxNDVSoFar = getMaxNDVFromProjections(colStatMap,
        jlpi.getProjsFromRightPartOfJoinKeysInJoinSchema(), maxNDVSoFar);

    return maxNDVSoFar;
  }

  private static Double getMaxNDVFromProjections(Map<Integer, Double> colStatMap,
      Set<Integer> projectionSet, Double defaultMaxNDV) {
    Double colNDV = null;
    Double maxNDVSoFar = defaultMaxNDV;

    for (Integer projIndx : projectionSet) {
      colNDV = colStatMap.get(projIndx);
      if (colNDV > maxNDVSoFar)
        maxNDVSoFar = colNDV;
    }

    return maxNDVSoFar;
  }
}
