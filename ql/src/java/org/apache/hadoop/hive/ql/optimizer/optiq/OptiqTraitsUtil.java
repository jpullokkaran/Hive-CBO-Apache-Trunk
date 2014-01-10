package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveAggregateRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveIRShuffleRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.MapJoinStreamingRelation;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveLimitRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveSortRel;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelFieldCollation.Direction;
import org.eigenbase.rel.RelFieldCollation.NullDirection;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

//TODO: handle expressions as partition/sort keys. It also implies that we need a way to represent expressions (One option is to add expression as virtual hidden columns of Shuffle/sort)
public class OptiqTraitsUtil {
/*
  private static boolean isJoinKeys(HiveJoinRel j, List<Integer> bucketCols) {
    boolean colsPartOfJoinKeys = true;

    JoinPredicateInfo jpi = j.getJoinPredicateInfo();
    if (bucketCols.size() != jpi.getJoinKeysFromLeftRelation().size()
        || bucketCols.size() != jpi.getJoinKeysFromRightRelation().size()) {
      colsPartOfJoinKeys = false;
    } else {
      for (int i = 0; i < bucketCols.size(); i++) {
        if (!jpi.getJoinKeysFromLeftRelation().get(i).equals(bucketCols.get(i))
            && !jpi.getJoinKeysFromRightRelation().get(i).equals(bucketCols.get(i))) {
          colsPartOfJoinKeys = false;
          break;
        }
      }
    }

    return colsPartOfJoinKeys;
  }
  */

  private static boolean propgateBucketTrait(HiveRel n, List<Integer> bucketCols,
      List<Integer> bucketSortCols) {
    boolean canPropagateBucketTrait = false;

    if (n instanceof HiveFilterRel) {
      if (propgateBucketTrait((HiveRel) n.getInput(0), bucketCols, bucketSortCols)) {
        ((HiveFilterRel) n).propagateBucketingTraitUpwardsViaTransformation(bucketCols,
            bucketSortCols);
        canPropagateBucketTrait = true;
      }
    } else if (n instanceof HiveLimitRel) {
      if (propgateBucketTrait((HiveRel) n.getInput(0), bucketCols, bucketSortCols)) {
        ((HiveLimitRel) n).propagateBucketingTraitUpwardsViaTransformation(bucketCols,
            bucketSortCols);
        canPropagateBucketTrait = true;
      }
    } else if (n instanceof HiveProjectRel) {
      List<Integer> translatedBucketingCols = OptiqUtil.translateProjIndxToChild(n, bucketCols);
      if (translatedBucketingCols != null) {
        if (bucketSortCols != null) {
          List<Integer> translatedBucketSortCols = OptiqUtil.translateProjIndxToChild(n,
              bucketSortCols);
          if (translatedBucketSortCols != null) {
            if (propgateBucketTrait((HiveRel) OptiqUtil.getNonSubsetRelNode(n.getInput(0)), translatedBucketingCols,
                translatedBucketSortCols)) {
              ((HiveProjectRel) n).propagateBucketingTraitUpwardsViaTransformation(
                  translatedBucketingCols,
                  translatedBucketSortCols);
              canPropagateBucketTrait = true;
            }
          }
        } else {
          if (propgateBucketTrait((HiveRel) n.getInput(0), translatedBucketingCols, null)) {
            ((HiveProjectRel) n).propagateBucketingTraitUpwardsViaTransformation(
                translatedBucketingCols, null);
            canPropagateBucketTrait = true;
          }
        }
      }
    } else if (n instanceof HiveJoinRel) {
      // TODO: uncomment & fix the code below
      /**
       * NOTE that here we don't check if bucketing cols are join keys. This is because we may
       * get transitive bucketing.
       * R1 - R2
       * | \
       * R4 R3
       * ((R1-R2)-R3)-R4
       */
      /*
       * HiveJoinRel j = (HiveJoinRel) n;
       * if (isJoinKeys(j, bucketCols)) {
       * if (bucketSortCols != null && !bucketSortCols.isEmpty()) {
       * if (isJoinKeys(j, bucketSortCols)) {
       * canPropagateBucketTrait = true;
       * }
       * } else {
       * canPropagateBucketTrait = true;
       * }
       * } else {
       * Pair<List<Integer>, List<Integer>> childBucketCols = OptiqUtil.translateProjIndxToChild(j,
       * bucketCols);
       * Pair<List<Integer>, List<Integer>> childBucketSortCols =
       * OptiqUtil.translateProjIndxToChild(j, bucketSortCols);
       * if (childBucketCols != null) {
       * if (childBucketCols.getFirst() != null)
       * }
       *
       * }
       */
      canPropagateBucketTrait = true;
    } else if (n instanceof HiveAggregateRel) {
      // TODO: Do this conditionally
      canPropagateBucketTrait = true;
    } else if (n instanceof HiveIRShuffleRel) {
      HiveIRShuffleRel shuffle = (HiveIRShuffleRel) n;
      if (shuffle.getPartCols().equals(bucketCols)) {
        canPropagateBucketTrait = true;
      }
    }

    return canPropagateBucketTrait;
  }

  private static boolean propgateSortTrait(HiveRel n,
      List<Integer> sortCols) {
    boolean canPropagateSortTrait = false;

    if (n instanceof HiveFilterRel) {
      if (propgateSortTrait((HiveRel) n.getInput(0), sortCols)) {
        ((HiveFilterRel) n).propagateSortingTraitUpwardsViaTransformation(sortCols);
        canPropagateSortTrait = true;
      }
    } else if (n instanceof HiveLimitRel) {
      if (propgateSortTrait((HiveRel) n.getInput(0), sortCols)) {
        ((HiveLimitRel) n).propagateSortingTraitUpwardsViaTransformation(sortCols);
        canPropagateSortTrait = true;
      }
    } else if (n instanceof HiveProjectRel) {
      List<Integer> translatedSortCols = OptiqUtil.translateProjIndxToChild(n,
          sortCols);
      if (translatedSortCols != null) {
        if (propgateSortTrait((HiveRel) n.getInput(0),
            translatedSortCols)) {
          ((HiveProjectRel) n).propagateSortingTraitUpwardsViaTransformation(
              translatedSortCols);
          canPropagateSortTrait = true;
        }
      }
    } else if (n instanceof HiveSortRel) {
      HiveSortRel s = (HiveSortRel) n;
      List<Integer> sortColsFromCollation = OptiqTraitsUtil.getSortingColPos(s.getCollation());
      if (sortColsFromCollation != null && OptiqUtil.orderedSubset(sortColsFromCollation, sortCols)) {
        canPropagateSortTrait = true;
      }
    }

    return canPropagateSortTrait;
  }

  public static void requestBucketTraitPropagationViaTransformation(HiveRel r,
      List<Integer> bucketCols, List<Integer> bucketSortCols) {
    if (r instanceof HiveFilterRel || r instanceof HiveLimitRel
        || r instanceof HiveProjectRel) {
      propgateBucketTrait(r, bucketCols, bucketSortCols);
    }
  }

  public static void requestSortTraitPropagationViaTransformation(HiveRel r,
      List<Integer> sortCols) {
    if (r instanceof HiveFilterRel || r instanceof HiveLimitRel
        || r instanceof HiveProjectRel) {
      propgateSortTrait(r, sortCols);
    }
  }

  public static RelTraitSet getSelectTraitSet(RelOptCluster cluster, List<RexNode> exps,
      RelNode child) {
    RelTrait childTrait = getPotentialPropagateableTraitFromChild(child);
    childTrait = translateToParent(cluster, exps, childTrait);
    return getCombinedTrait(cluster, childTrait);
  }

  public static RelTraitSet getSortTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelCollation collation) {
    if (traitSet == null) {
      return getCombinedTrait(cluster, collation);
    }
    else {
      traitSet.add(collation);
      return traitSet;
    }
  }

  public static RelTraitSet getFilterTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child) {
    return getSingleRelTraitSet(cluster, traitSet, child);
  }

  public static RelTraitSet getLimitTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child) {
    return getSingleRelTraitSet(cluster, traitSet, child);
  }

  public static RelTraitSet getAggregateTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      List<Integer> gbCols, List<AggregateCall> aggCalls, RelNode child) {
    // TODO: verify that GB will always sort data in ascending order and NULLS direction
    RelCollation collation = getCollation(gbCols, Direction.Ascending, NullDirection.FIRST);
    RelBucketing bucketingtrait = RelBucketingTraitImpl.of(gbCols, collation, 0, 0.0);
    // new RelBucketingTraitImpl(gbCols, collation, 0, 0.0);
    if (traitSet == null) {
      return getCombinedTrait(cluster, bucketingtrait);
    } else {
      traitSet = traitSet.plus(bucketingtrait);
      return traitSet;
    }
  }


  public static RelTraitSet getTableScanTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptHiveTable table,
      RelDataType rowtype) {
    if (traitSet == null) {
      return getCombinedTrait(cluster, table.getBucketTrait(rowtype));
    }
    else {
    	RelBucketing bTrait = table.getBucketTrait(rowtype);
    	if ( bTrait != null ) {
    		traitSet.add(bTrait);
    	}
      return traitSet;
    }
  }

  public static RelTraitSet getJoinTraitSet(RelOptCluster cluster, RelTraitSet traitSet) {
    RelTraitSet traitSetFromChild = getCombinedTrait(cluster, null);
    if (traitSet != null) {
      traitSetFromChild.merge(traitSet);
    }
    return traitSetFromChild;
  }

  // TODO: Shuffle join will carry forward the bucketing trait (assumption here is both sides have
  // equal number of buckets)
  public static RelTraitSet getShuffleJoinTraitSet(HiveJoinRel j) {
    return getSortMergeJoinTraitSet(j);
  }

  public static RelTraitSet getMapJoinTraitSet(HiveJoinRel j) {
    return getHashJoinTraitSet(j, false);
  }

  public static RelTraitSet getBucketJoinTraitSet(HiveJoinRel j) {
    return getHashJoinTraitSet(j, false);
  }

  public static RelTraitSet getSMBJoinTraitSet(HiveJoinRel j) {
    return getHashJoinTraitSet(j, true);
  }

  /**
   * NOTE: We don't check for predicates left around on join conditions (i.e non join keys). This
   * should
   * happen only for outer joins and we assume that if "f(x) <op> P" implies "f(y) <op> P" where x
   * is join key from one side and y is join key from other side.
   *
   * @param j
   *          HiveJoin
   * @return
   */
  private static RelTraitSet getSortMergeJoinTraitSet(HiveJoinRel j) {
	  	  
    HiveRel left = OptiqUtil.getNonSubsetRelNode(j.getLeft());
    HiveRel right = OptiqUtil.getNonSubsetRelNode(j.getRight());
    RelBucketing leftBucketingTrait = getBucketingTrait(left.getTraitSet());
    RelBucketing rightBucketingTrait = getBucketingTrait(right.getTraitSet());
    Map<Integer, Integer> leftChildToParentProjMap = OptiqUtil
        .translateChildColPosToParent(j, true);
    Map<Integer, Integer> rightChildToParentProjMap = OptiqUtil.translateChildColPosToParent(j,
        false);

    RelBucketing leftBucketTraitTranslatedToParent = translateBucketingTraitToParent(
        leftBucketingTrait, leftChildToParentProjMap);
    RelBucketing rightBucketTraitTranslatedToParent = translateBucketingTraitToParent(
        rightBucketingTrait, rightChildToParentProjMap);
    Set<ImmutableList<Integer>> partitioningColsSet = new HashSet<ImmutableList<Integer>>();
    Set<RelCollation> collationset = new HashSet<RelCollation>();

    partitioningColsSet.addAll(leftBucketTraitTranslatedToParent.getPartitionCols());
    partitioningColsSet.addAll(rightBucketTraitTranslatedToParent.getPartitionCols());
    collationset.addAll(leftBucketingTrait.getCollation());
    collationset.addAll(rightBucketingTrait.getCollation());

    return getCombinedTrait(j.getCluster(), RelBucketingTraitImpl.of(
        ImmutableSet.copyOf(partitioningColsSet), ImmutableSet.copyOf(collationset),
        leftBucketTraitTranslatedToParent.getNumberOfBuckets(),
        leftBucketTraitTranslatedToParent.getSizeOfLargestBucket()));
  }

  // NOTE: For Bucket Join only streaming side Preserves bucketing
  // trait unless the number of buckets of both sides are equal
  private static RelTraitSet getHashJoinTraitSet(HiveJoinRel j, boolean enforceSorting) {
    RelTrait childTraitTranslatedToParent = null;
    RelNode streamingChildNode = null;
    Map<Integer, Integer> ChildToParentProjMap = null;
    boolean childLeftOfJoin = false;
    if (j.getMapJoinStreamingSide() == MapJoinStreamingRelation.LEFT_RELATION) {
      streamingChildNode = j.getLeft();
      childLeftOfJoin = true;
    } else if (j.getMapJoinStreamingSide() == MapJoinStreamingRelation.RIGHT_RELATION) {
      streamingChildNode = j.getRight();
    }

    if (streamingChildNode != null) {
      RelTrait childTrait = getPotentialPropagateableTraitFromChild(streamingChildNode);

      if (childTrait != null) {
        if (isBucketingTrait(childTrait)) {
          RelBucketing childBucketingTrait = (RelBucketing) childTrait;
          ChildToParentProjMap = OptiqUtil.translateChildColPosToParent(j, childLeftOfJoin);
          JoinPredicateInfo jpi = OptiqUtil.getJoinPredicateInfo(j);
          if (jpi.getNonJoinKeyLeafPredicates() == null) {
            List<Integer> joinKeysOfInterestInChildNode = null;
            List<Integer> joinKeysOfInterestInJoin = null;

            if (childLeftOfJoin) {
              joinKeysOfInterestInChildNode = jpi.getJoinKeysFromLeftRelation();
              joinKeysOfInterestInJoin = jpi.getJoinKeysInJoinSchemaFromLeft();
            } else {
              joinKeysOfInterestInChildNode = jpi.getJoinKeysFromRightRelation();
              joinKeysOfInterestInJoin = jpi.getJoinKeysInJoinSchemaFromRight();
            }

            RelBucketing tmp = translateBucketingTraitToParent(
                childBucketingTrait, ChildToParentProjMap, joinKeysOfInterestInChildNode);

            if (tmp.getPartitionCols().contains(joinKeysOfInterestInJoin)
                && (enforceSorting && tmp.getSortingCols().contains(joinKeysOfInterestInJoin))) {
              childTraitTranslatedToParent = tmp;
            }
          }
        } else if (isSortingTrait(childTrait)) {
          RelCollation childSortingTrait = (RelCollation) childTrait;

          ChildToParentProjMap = OptiqUtil.translateChildColPosToParent(j, childLeftOfJoin);
          JoinPredicateInfo jpi = OptiqUtil.getJoinPredicateInfo(j);
          if (jpi.getNonJoinKeyLeafPredicates() == null) {
            List<Integer> joinKeysOfInterestInChildNode = null;

            if (childLeftOfJoin) {
              joinKeysOfInterestInChildNode = jpi.getJoinKeysFromLeftRelation();
            } else {
              joinKeysOfInterestInChildNode = jpi.getJoinKeysFromRightRelation();
            }

            RelCollation tmp = translateSortingTraitToParent(
                childSortingTrait, ChildToParentProjMap, joinKeysOfInterestInChildNode);

            if (tmp != null) {
              childTraitTranslatedToParent = tmp;
            }
          }
        }
      }
    }

    return getCombinedTrait(j.getCluster(), childTraitTranslatedToParent);
  }

  /**
   * Union Can not preserve Bucketing or Sorting trait
   *
   * @param cluster
   * @param inputs
   * @param all
   * @param child
   * @return
   */
  public static RelTraitSet getUnionTraitSet(RelOptCluster cluster,
      List<RelNode> inputs,
      boolean all) {

    return getCombinedTrait(cluster, null);
  }

  /**
   * Shuffle introduces Bucketing trait, hence can not carry forward anything from child.
   *
   * @param irShuffleTraitSet
   * @param child
   * @return
   */
  public static RelTraitSet getIRShuffleApplicableTraitSet(RelOptCluster cluster,
      List<Integer> shuffleKeys,
      RelNode child) {
    // TODO: Confirm shuffle is always gonna shuffle in ascending order & NULL direction doesn't
    // matter
    // TODO: handle shuffle keys being compound expression (R1 join R2 on (R1.x + R1.y) = (R2.x
    // +R2.y). In this case we Shuffle needs to have a virtual column representing R1.X + R1.y and
    // this should form the partition & collation keys
    RelCollation collation = getCollation(shuffleKeys, Direction.Ascending,
        NullDirection.UNSPECIFIED);
    return getCombinedTrait(cluster, RelBucketingTraitImpl.of(shuffleKeys, collation, 0, 0.0));
  }

  public static RelTraitSet getIRBroadcastApplicableTraitSet(RelOptCluster cluster,
      RelTraitSet traitSet, RelNode child) {
    RelTraitSet traitSetFromchild = null;
    RelTrait childTrait = getPotentialPropagateableTraitFromChild(child);
    if (!isSortingTrait(childTrait)) {
      childTrait = null;
    }
    traitSetFromchild = getCombinedTrait(cluster, childTrait);
    if (traitSet != null) {
      traitSetFromchild.merge(traitSet);
    }

    return traitSetFromchild;
  }

  public static RelBucketing getBucketingTrait(RelTraitSet traitset) {
    RelTrait tmpTrait = null;
    RelBucketing bucketTrait = null;
    Iterator<RelTrait> traitIterator = traitset.iterator();

    while (traitIterator.hasNext()) {
      tmpTrait = traitIterator.next();
      if (isBucketingTrait(tmpTrait)) {
        bucketTrait = (RelBucketing) tmpTrait;
        break;
      }
    }

    return bucketTrait;
  }

  public static RelCollation getSortTrait(RelTraitSet traitset) {
    RelTrait tmpTrait = null;
    RelCollation sortTrait = null;
    Iterator<RelTrait> traitIterator = traitset.iterator();

    while (traitIterator.hasNext()) {
      tmpTrait = traitIterator.next();
      if (isSortingTrait(tmpTrait)) {
        sortTrait = (RelCollation) tmpTrait;
        break;
      }
    }

    return sortTrait;
  }

  public static boolean isBucketingTrait(RelTrait trait) {
    if (trait.getTraitDef().getSimpleName().equals(RelBucketingTraitDef.SIMPLE_NAME)) {
      return true;
    }
    return false;
  }

  public static boolean isSortingTrait(RelTrait trait) {
    if (trait.getTraitDef().getSimpleName().equals("sort")) {
      return true;
    }
    return false;
  }

  public static RelTraitSet getHiveTraitSet(RelOptCluster cluster, RelTraitSet traitSet) {
    RelTraitSet hiveTraitSet = null;

    hiveTraitSet = getCombinedTrait(cluster, null);
    if (traitSet != null) {
      hiveTraitSet.merge(traitSet);
    }

    return hiveTraitSet;
  }

  private static RelTraitSet getSingleRelTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child) {
    if (traitSet == null) {
      return getCombinedTrait(cluster, getPotentialPropagateableTraitFromChild(child));
    }
    else {
      RelTrait childTrait = getPotentialPropagateableTraitFromChild(child);
      traitSet.add(childTrait);
      return traitSet;
    }
  }

  private static RelTraitSet getCombinedTrait(RelOptCluster cluster, RelTrait trait) {
    RelTraitSet traitSet = null;

    if (trait != null) {
      traitSet = cluster.traitSetOf(HiveRel.CONVENTION, RelCollationTraitDef.INSTANCE.getDefault(), trait);
    } else {
      traitSet = cluster.traitSetOf(HiveRel.CONVENTION, RelCollationTraitDef.INSTANCE.getDefault());
    }

    return traitSet;
  }

  /**
   * The only traits that are propagate
   *
   * @param child
   * @return
   */
  private static RelTrait getPotentialPropagateableTraitFromChild(RelNode child) {
    RelTraitSet childTrait = child.getTraitSet();
    RelTrait propagateableTrait = null;
    Iterator<RelTrait> itr = childTrait.iterator();
    RelTrait tmpTrait;
    while (itr.hasNext()) {
      tmpTrait = itr.next();
      if (tmpTrait.getTraitDef().getSimpleName().equals(RelBucketingTraitDef.SIMPLE_NAME)
          || tmpTrait.getTraitDef().getSimpleName().equals("sort")) {
        propagateableTrait = tmpTrait;
        break;
      }
    }

    return propagateableTrait;
  }

  private static ImmutableSet<RelCollation> getCollation(
      ImmutableSet<ImmutableList<Integer>> colPosLstImmutableSet, Direction nonNullsortOrder,
      NullDirection nullSortOrder) {
    Set<RelCollation> collationSet = new HashSet<RelCollation>();
    RelCollation tmp = null;
    for (ImmutableList<Integer> colPosLst : colPosLstImmutableSet) {
      tmp = getCollation(colPosLst, nonNullsortOrder, nullSortOrder);
      if (tmp != null) {
        collationSet.add(tmp);
      }
    }

    if (!collationSet.isEmpty()) {
      return ImmutableSet.copyOf(collationSet);
    } else {
      return null;
    }
  }

  private static RelCollation getCollation(List<Integer> colPosLst, Direction nonNullsortOrder,
      NullDirection nullSortOrder) {
    List<RelFieldCollation> filedCollations = new LinkedList<RelFieldCollation>();
    for (Integer colPos : colPosLst) {
      filedCollations.add(new RelFieldCollation(colPos, nonNullsortOrder, nullSortOrder));
    }
    return RelCollationImpl.of(filedCollations);
  }

  public static List<Integer> getSortingColPos(RelCollation collation) {
    List<Integer> sortingColPosLst = new LinkedList<Integer>();
    for (RelFieldCollation col : collation.getFieldCollations()) {
      sortingColPosLst.add(col.getFieldIndex());
    }
    return sortingColPosLst;
  }

  private static RelBucketing translateBucketingTraitToParent(RelBucketing childBucketingTrait,
      Map<Integer, Integer> projMapFromChildToParent, List<Integer> bucketingColsInChild) {
    ImmutableSet<ImmutableList<Integer>> partColsInParent = null;
    ImmutableSet<ImmutableList<Integer>> sortColsInParent = null;
    ImmutableSet<RelCollation> collationset = null;

    if (childBucketingTrait.getPartitionCols().contains(bucketingColsInChild)) {
      partColsInParent = OptiqUtil.translateChildColPosToParent(projMapFromChildToParent,
          childBucketingTrait.getPartitionCols(), false);
    }
    if (partColsInParent != null && !partColsInParent.isEmpty()) {
      if (childBucketingTrait.getCollation() != null
          && !childBucketingTrait.getCollation().isEmpty()
          && childBucketingTrait.getSortingCols().contains(bucketingColsInChild)) {
        sortColsInParent = OptiqUtil.translateChildColPosToParent(projMapFromChildToParent,
            childBucketingTrait.getSortingCols(), false);
        collationset = getCollation(sortColsInParent, childBucketingTrait.getNonNullSortOrder(),
            childBucketingTrait.getNullSortOrder());
      }
      return RelBucketingTraitImpl.of(partColsInParent, collationset,
          childBucketingTrait.getNumberOfBuckets(), childBucketingTrait.getSizeOfLargestBucket());
    }

    return null;
  }

  private static RelBucketing translateBucketingTraitToParent(RelBucketing childBucketingTrait,
      Map<Integer, Integer> projMapFromChildToParent) {
    ImmutableSet<ImmutableList<Integer>> partColsInParent = null;
    ImmutableSet<ImmutableList<Integer>> sortColsInParent = null;
    ImmutableSet<RelCollation> collationset = null;

    partColsInParent = OptiqUtil.translateChildColPosToParent(projMapFromChildToParent,
        childBucketingTrait.getPartitionCols(), false);
    if (partColsInParent != null && !partColsInParent.isEmpty()) {
      if (childBucketingTrait.getCollation() != null
          && !childBucketingTrait.getCollation().isEmpty()) {
        sortColsInParent = OptiqUtil.translateChildColPosToParent(projMapFromChildToParent,
            childBucketingTrait.getSortingCols(), false);
        collationset = getCollation(sortColsInParent, childBucketingTrait.getNonNullSortOrder(),
            childBucketingTrait.getNullSortOrder());
      }
      return RelBucketingTraitImpl.of(partColsInParent, collationset,
          childBucketingTrait.getNumberOfBuckets(), childBucketingTrait.getSizeOfLargestBucket());
    }

    return null;
  }

  private static RelCollation translateSortingTraitToParent(RelCollation childCollationTrait,
      Map<Integer, Integer> projMapFromChildToParent) {
    List<Integer> sortColsInParent = null;

    sortColsInParent = OptiqUtil.translateChildColPosToParent(projMapFromChildToParent,
        getSortingColPos(childCollationTrait), true);
    if (sortColsInParent != null && !sortColsInParent.isEmpty()) {
      return getCollation(sortColsInParent,
          childCollationTrait.getFieldCollations().get(0).direction,
          childCollationTrait.getFieldCollations().get(0).nullDirection);
    }

    return null;
  }

  private static RelCollation translateSortingTraitToParent(RelCollation childCollationTrait,
      Map<Integer, Integer> projMapFromChildToParent, List<Integer> sortingColsInChild) {
    List<Integer> sortColsInParent = null;

    sortColsInParent = OptiqUtil.translateChildColPosToParent(projMapFromChildToParent,
        getSortingColPos(childCollationTrait), false);
    if (sortColsInParent != null && !sortColsInParent.isEmpty()) {
      return getCollation(sortColsInParent,
          childCollationTrait.getFieldCollations().get(0).direction,
          childCollationTrait.getFieldCollations().get(0).nullDirection);
    }

    return null;
  }

  private static RelTrait translateToParent(RelOptCluster cluster, List<RexNode> exps,
      RelTrait childTrait) {
    RelTrait traitInParent = null;

    if (childTrait != null) {
      Map<Integer, Integer> projMapFromChildToParent = OptiqUtil.translateChildColPosToParent(exps);

      if (isBucketingTrait(childTrait)) {
        traitInParent = translateBucketingTraitToParent((RelBucketing) childTrait,
            projMapFromChildToParent);
      } else if (isSortingTrait(childTrait)) {
        traitInParent = translateSortingTraitToParent((RelCollation) childTrait,
            projMapFromChildToParent);
      }
    }

    return traitInParent;
  }
}
