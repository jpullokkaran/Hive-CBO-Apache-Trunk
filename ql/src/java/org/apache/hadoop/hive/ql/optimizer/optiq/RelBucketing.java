package org.apache.hadoop.hive.ql.optimizer.optiq;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation.Direction;
import org.eigenbase.rel.RelFieldCollation.NullDirection;
import org.eigenbase.relopt.RelTrait;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * The bucketing trait.
 *
 * @see org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketingTraitDef
 */
public interface RelBucketing extends RelTrait {

  public Integer getNumberOfBuckets();

  // REVIEW: This is stats. It should not be in a trait.
  public Double getSizeOfLargestBucket();

  public ImmutableSet<ImmutableList<Integer>> getPartitionCols();

  public ImmutableSet<RelCollation> getCollation();

  public ImmutableSet<ImmutableList<Integer>> getSortingCols();

  public Direction getNonNullSortOrder();

  public NullDirection getNullSortOrder();

  public boolean noOfBucketsMultipleOfEachOther(RelBucketing bucketTraitToCompare);
}