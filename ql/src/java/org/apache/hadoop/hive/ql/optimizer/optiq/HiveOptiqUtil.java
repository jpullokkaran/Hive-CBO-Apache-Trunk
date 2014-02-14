package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.volcano.RelSubset;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Generic utility functions needed for Optiq based Hive CBO.
 */

public class HiveOptiqUtil {

  /**
   * If RelNode is a RelSubSet then get the best RelNode from the subset.
   * <p>
   * This seems to be against the Optiq design. Current/original intent of this
   * method was to use for getting Stats from Child (Cardinality, Column Stats,
   * Memory Usage). This should probably be moved RelMetadataProvider
   * 
   * @param n
   * @return HiveRel
   */
  public static HiveRel getNonSubsetRelNode(RelNode n) {
    if (false)
      throw new AssertionError(); // THIS METHOD IS EVIL
    if (n instanceof RelSubset) {
      return ((HiveRel) ((RelSubset) n).getBest());
    } else {
      return ((HiveRel) n);
    }
  }

  /**
   * Get list of virtual columns from the given list of projections.
   * <p>
   * 
   * @param exps
   *          list of rex nodes representing projections
   * @return List of Virtual Columns, will not be null.
   */
  public static List<Integer> getVirtualCols(List<RexNode> exps) {
    List<Integer> vCols = new ArrayList<Integer>();

    for (int i = 0; i < exps.size(); i++) {
      if (!(exps.get(i) instanceof RexInputRef)) {
        vCols.add(i);
      }
    }

    return vCols;
  }

  public static List<Integer> translateBitSetToProjIndx(BitSet projBitSet) {
    List<Integer> projIndxLst = new ArrayList<Integer>();

    for (int i = 0; i < projBitSet.length(); i++) {
      if (projBitSet.get(i)) {
        projIndxLst.add(i);
      }
    }

    return projIndxLst;
  }

  /**
   * Partition given list of Join's Project Indexes to those coming from left
   * and right child. Also optionally translate project indexes in terms of its
   * left and right children.
   * <P>
   * It is assumed that Join will not contain any virtual columns.
   * 
   * @param parentJoin
   *          Join Operator of interest
   * @param projIndxInParentLst
   *          List of projection indexes in Join Schema that needs to be
   *          partitioned and optionally translated.
   * @param translateToChildSchema
   *          If true projIndxInParentLst will be partitioned in to projections
   *          from left and right and would be translated to indexes of its left
   *          and right child.
   * @return Pair of Immutable List of Projection indexes: <Left, Right>
   *         (Translated to child if translateToChildSchema is true).
   */
  public static Pair<ImmutableList<Integer>, ImmutableList<Integer>> partitionProjIndxes(
      HiveJoinRel parentJoin, List<Integer> projIndxInParentLst, boolean translateToChildSchema) {
    if (projIndxInParentLst == null) {
      return null;
    }

    todo("Move this to Optiq");

    ImmutableList.Builder<Integer> leftChildProjListBuilder = ImmutableList.builder();
    ImmutableList.Builder<Integer> rightChildProjListBuilder = ImmutableList.builder();

    int rightOffSet = parentJoin.getLeft().getRowType().getFieldCount();
    for (Integer projIndx : projIndxInParentLst) {
      if (projIndx < rightOffSet) {
        leftChildProjListBuilder.add(projIndx);
      } else {
        if (translateToChildSchema)
          rightChildProjListBuilder.add(projIndx - rightOffSet);
        else
          rightChildProjListBuilder.add(projIndx);
      }
    }

    return new Pair<ImmutableList<Integer>, ImmutableList<Integer>>(
        leftChildProjListBuilder.build(), rightChildProjListBuilder.build());
  }

  /**
   * Fill ImmutableMap with Keys and their corresponding values.
   * <p>
   * 
   * @param mapToFill
   *          Map to Fill
   * @param listOfKey
   *          List of Keys
   * @param listOfvalue
   *          List of values corresponding to Keys.
   */
  public static <T1, T2> void fillMap(ImmutableMap.Builder<T1, T2> mapToFill, List<T1> listOfKey,
      List<T2> listOfvalue) {
    for (int i = 0; i < listOfKey.size(); i++) {
      mapToFill.put(listOfKey.get(i), listOfvalue.get(i));
    }
  }

  @Deprecated
  public static void todo(String s) {
  }
}
