package org.apache.hadoop.hive.ql.optimizer.optiq.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.optimizer.optiq.HiveOptiqJoinUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.HiveOptiqJoinUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.HiveOptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.Pair;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.JoinRelType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Utility for stats related stuff.<br>
 * <p>
 * Main Elements:<br>
 * 1. Selectivity determination.<br>
 * 2. Column Stats computation (by computing NDV).
 * 
 * TODO: Move this to Optiq Framework
 */

public class HiveOptiqStatsUtil {

  /*** Selectivity ***/
  /**
   * Compute Join Selectivity.
   * 
   * @param j
   *          HiveJoinRel
   * @return Join Selectivity, will be within 0 and 1.
   * 
   *         TODO: support one sided, full outer join and semi join
   */
  public static double computeJoinSelectivity(HiveJoinRel j) {
    if (j.getJoinType().equals(JoinRelType.INNER)) {
      return computeInnerJoinSelectivity(j);
    }
    return 1;
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
   *         TODO: handle join conditions like "(r1.x=r2.x) and (r1.x=r2.y)"
   *         better
   */
  private static double computeInnerJoinSelectivity(HiveJoinRel j) {
    double joinSelectivity = 1;
    JoinPredicateInfo jpi = j.getJoinPredicateInfo();
    HiveRel left = HiveOptiqUtil.getNonSubsetRelNode(j.getLeft());
    HiveRel right = HiveOptiqUtil.getNonSubsetRelNode(j.getRight());
    ImmutableMap.Builder<Integer, HiveColStat> colStatMapBuilder = ImmutableMap.builder();
    ImmutableMap<Integer, HiveColStat> colStatMap = null;

    double tmpDouble;
    List<Integer> tmpProjIndxLst = new ArrayList<Integer>();
    List<HiveColStat> tmpColStatsLst = new ArrayList<HiveColStat>();

    HiveOptiqUtil.todo("To use Optiq RelMDSelecetivity");

    // 1. Update Col Stats Map with col stats for columns from left side of
    // Join which are part of join keys
    tmpProjIndxLst.addAll(jpi.getProjsFromLeftPartOfJoinKeysInChildSchema());
    tmpColStatsLst.addAll(left.getColStat(tmpProjIndxLst));
    HiveOptiqUtil.fillMap(colStatMapBuilder, tmpProjIndxLst, tmpColStatsLst);

    // 2. Update Col Stats Map with col stats for columns from right side of
    // Join which are part of join keys
    tmpProjIndxLst.clear();
    tmpColStatsLst.clear();
    tmpProjIndxLst.addAll(jpi.getProjsFromRightPartOfJoinKeysInChildSchema());
    tmpColStatsLst.addAll(right.getColStat(tmpProjIndxLst));
    tmpProjIndxLst.clear();
    tmpProjIndxLst.addAll(jpi.getProjsFromRightPartOfJoinKeysInJoinSchema());
    HiveOptiqUtil.fillMap(colStatMapBuilder, tmpProjIndxLst, tmpColStatsLst);

    colStatMap = colStatMapBuilder.build();

    // 3. Walk through the Join Condition Building Selectivity
    // Join Selectivity for inner join with Join Condition
    // "(left.expr1 = right.expr1) && (left.expr2 = right.expr2)"
    // = 1/(max(NDV(left.Expr1), NDV(right.Expr1)) * max(NDV(left.Expr2),
    // NDV(right.Expr2)))
    // NDV(expr) = max(NDV( expr args))
    for (JoinLeafPredicateInfo jlpi : jpi.getEquiJoinPredicateElements()) {
      tmpDouble = (1 / ((double)getMaxNDVForJoinSelectivity(jlpi, colStatMap)));
      joinSelectivity *= tmpDouble;
    }

    return joinSelectivity;
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
  private static long getMaxNDVForJoinSelectivity(JoinLeafPredicateInfo jlpi,
      ImmutableMap<Integer, HiveColStat> colStatMap) {
    long maxNDVSoFar = 1;

    maxNDVSoFar = getMaxNDVFromProjections(colStatMap,
        jlpi.getProjsFromLeftPartOfJoinKeysInJoinSchema(), maxNDVSoFar);
    maxNDVSoFar = getMaxNDVFromProjections(colStatMap,
        jlpi.getProjsFromRightPartOfJoinKeysInJoinSchema(), maxNDVSoFar);

    return maxNDVSoFar;
  }

  private static long getMaxNDVFromProjections(Map<Integer, HiveColStat> colStatMap,
      Set<Integer> projectionSet, long defaultMaxNDV) {
    HiveColStat colStat = null;
    long maxNDVSoFar = defaultMaxNDV;

    for (Integer projIndx : projectionSet) {
      colStat = colStatMap.get(projIndx);
      if (colStat.getNDV() > maxNDVSoFar)
        maxNDVSoFar = colStat.getNDV();
    }

    return maxNDVSoFar;
  }

  /*** Col Stats ***/

  /**
   * Convenience method for computing Column Stats for given index.
   * @param rel
   * @param projIndx
   * @return
   */
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

  /**
   * Get Column Stats for given projection indexes for given Hive Join.
   * 
   * @param j
   * @param projIndxLst
   *          List of projection indexes of Join.
   * @return Immutable List of Column Stats
   */
  public static ImmutableList<HiveColStat> getJoinRelColStat(HiveJoinRel j,
      List<Integer> projIndxLst) {
    Map<Integer, HiveColStat> colStatMapForJoin;
    ImmutableMap<Integer, HiveColStat> colStatMapFromChild;
    JoinPredicateInfo jpi = j.getJoinPredicateInfo();
    Set<Integer> joinKeysFromLeftInJoinSchema = new HashSet<Integer>(
        jpi.getProjsFromLeftPartOfJoinKeysInJoinSchema());
    Set<Integer> joinKeysFromRightInJoinSchema = new HashSet<Integer>(
        jpi.getProjsFromRightPartOfJoinKeysInJoinSchema());
    Map<Integer, ImmutableList<JoinLeafPredicateInfo>> mapJoinKeyToJLPILst = jpi
        .getMapOfProjIndxToLeafPInfo();
    ImmutableList.Builder<HiveColStat> colStatsListBuilder = ImmutableList.builder();

    // 1. Get Col Stats from Child & build a transient map of Col Stats for Join
    colStatMapFromChild = getColStatMap(j, projIndxLst);
    colStatMapForJoin = new HashMap<Integer, HiveColStat>(colStatMapFromChild);

    // 2. Determine if Join Keys are part of user requested list
    joinKeysFromLeftInJoinSchema.retainAll(projIndxLst);
    joinKeysFromRightInJoinSchema.retainAll(projIndxLst);

    // 3. Update transient map with Colstats for keys from left
    if (!joinKeysFromLeftInJoinSchema.isEmpty())
      updateJoinColStatsMap(joinKeysFromLeftInJoinSchema, false, colStatMapForJoin,
          colStatMapFromChild, mapJoinKeyToJLPILst);

    // 4. Update transient map with Colstats for keys from right
    if (!joinKeysFromRightInJoinSchema.isEmpty())
      updateJoinColStatsMap(joinKeysFromRightInJoinSchema, true, colStatMapForJoin,
          colStatMapFromChild, mapJoinKeyToJLPILst);

    // 5. Build Immutable Map of Projection Indexes - ColStats that caller asked
    // for
    for (Integer projIndx : projIndxLst) {
      colStatsListBuilder.add(colStatMapForJoin.get(projIndx));
    }

    return colStatsListBuilder.build();
  }

  /**
   * Get Immutable map of Projection Index(in Join Schema) to Column Stats.
   * <p>
   * 
   * @param j
   *          Join of interest
   * @param projIndxLst
   *          List of Projection indexes of Join for which me need column stats
   * @return Map of map of Projection Index(in Join Schema) to Column Stats
   */
  private static ImmutableMap<Integer, HiveColStat> getColStatMap(HiveJoinRel j,
      List<Integer> projIndxLst) {
    HiveRel left = HiveOptiqUtil.getNonSubsetRelNode(j.getLeft());

    // 1. Partition requested Projection Indexes to those from Left and Right in
    // Join Schema and Child Schema
    Pair<ImmutableList<Integer>, ImmutableList<Integer>> projIndxInChildSchemaLstPair = HiveOptiqUtil
        .partitionProjIndxes(j, projIndxLst, true);
    Pair<ImmutableList<Integer>, ImmutableList<Integer>> projIndxInJoinSchemaLstPair = HiveOptiqUtil
        .partitionProjIndxes(j, projIndxLst, false);

    List<Integer> projInLeftSchema = projIndxInChildSchemaLstPair.getFirst();
    List<Integer> projFromLeftInParentSchema = projIndxInJoinSchemaLstPair.getFirst();
    List<Integer> projInRightSchema = projIndxInChildSchemaLstPair.getSecond();
    List<Integer> projFromRightInParentSchema = projIndxInJoinSchemaLstPair.getSecond();
    List<HiveColStat> tmpColStatsLst = new LinkedList<HiveColStat>();
    ImmutableMap.Builder<Integer, HiveColStat> colStatMapBuilder = ImmutableMap.builder();

    // 2. Add all col stats from Left to the map
    if (!projInLeftSchema.isEmpty()) {
      tmpColStatsLst.addAll(left.getColStat(projInLeftSchema));
      HiveOptiqUtil.fillMap(colStatMapBuilder, projFromLeftInParentSchema, tmpColStatsLst);
    }

    // 3. Add all col stats from Right to the map
    tmpColStatsLst.clear();
    if (!projInRightSchema.isEmpty()) {
      tmpColStatsLst.addAll(left.getColStat(projInRightSchema));
      HiveOptiqUtil.fillMap(colStatMapBuilder, projFromRightInParentSchema, tmpColStatsLst);
    }

    // 4. Return Immutable Map of Projection Indexes to Hive Col Stat
    return colStatMapBuilder.build();
  }

  /**
   * For a given projection of join which is part of join key, get max NDV.
   * <p>
   * NDV of x where x is part of Join Key f(x, y, z) = f(p, q, r) is
   * "max(NDV(p, q, r))"
   * 
   * @param jlpi
   *          Join Leaf Predicate Info
   * @param colStatMapFromChild
   *          Map of Projection Index (in Join Schema) to Column Stat
   * @param jkInJoinSchema
   *          Join Key projection Index for which NDV is needed
   * @param useKeysFromRight
   *          Whether we want to get NDV by looking at join keys coming from
   *          right
   * @return long, max NDV
   */
  private static long getMaxNDVForJoinColStat(JoinLeafPredicateInfo jlpi,
      Map<Integer, HiveColStat> colStatMapFromChild, Integer jkInJoinSchema, boolean useKeysFromRight) {
    Set<Integer> joinKeysInJoinSchemaFromOtherSide = null;

    if (!useKeysFromRight) {
      joinKeysInJoinSchemaFromOtherSide = jlpi.getProjsFromLeftPartOfJoinKeysInJoinSchema();
    } else {
      joinKeysInJoinSchemaFromOtherSide = jlpi.getProjsFromRightPartOfJoinKeysInJoinSchema();
    }

    return getMaxNDVFromProjections(colStatMapFromChild, joinKeysInJoinSchemaFromOtherSide,
        colStatMapFromChild.get(jkInJoinSchema).getNDV());
  }

  /***
   * Update Col Stats for Join Keys, in JoinSchema, by computing their NDV.
   * <p>
   * @param joinKeysInJoinSchema For which we need to update the stats in the Map.
   * @param useKeysFromRight Should we use JoinKeys from right side of corresponding JoinLeafPredicateInfo.
   * @param colStatMapForJoin Transient Map that needs to be updated.
   * @param colStatMapFromChild Reference map of col stats.
   * @param mapJoinKeyToJLPILst Map of join Key to JoinLeafPredicateInfo.
   */
  private static void updateJoinColStatsMap(Set<Integer> joinKeysInJoinSchema, boolean useKeysFromRight,
      Map<Integer, HiveColStat> colStatMapForJoin,
      ImmutableMap<Integer, HiveColStat> colStatMapFromChild,
      Map<Integer, ImmutableList<JoinLeafPredicateInfo>> mapJoinKeyToJLPILst) {
    long maxNDv;
    long tmpMaxNDV;
    HiveColStat tmpColStat;
    int projIndx = 0;
    List<JoinLeafPredicateInfo> tmpJLPILst = null;

    for (Integer jkInJoinSchema : joinKeysInJoinSchema) {
      // 1. Get List of Join leaf predicates that this key belongs to
      tmpJLPILst = mapJoinKeyToJLPILst.get(jkInJoinSchema);
      
      // 2. Use the current NDV in the transient map as the default
      tmpColStat = colStatMapForJoin.get(jkInJoinSchema);
      maxNDv = tmpMaxNDV = tmpColStat.getNDV();
      
      // 3. Walk through the List of Join Leaf Predicates and compute MaxNDV 
      for (JoinLeafPredicateInfo jlpi : tmpJLPILst) {
        tmpMaxNDV = getMaxNDVForJoinColStat(jlpi, colStatMapFromChild, jkInJoinSchema, useKeysFromRight);
        if (tmpMaxNDV < maxNDv)
          maxNDv = tmpMaxNDV;
      }
      colStatMapForJoin.put(projIndx, new HiveColStat(tmpColStat.getAvgSz(), maxNDv));
    }
  }
}
