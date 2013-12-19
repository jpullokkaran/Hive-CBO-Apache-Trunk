package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelFieldCollation.Direction;
import org.eigenbase.rel.RelFieldCollation.NullDirection;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitDef;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class RelBucketingTraitImpl implements RelBucketing {
  private final Integer m_noOfBuckets;
  private final Double m_szOfLargestBucket;
  private final ImmutableSet<ImmutableList<Integer>> m_partitioningColsSet;
  private final ImmutableSet<ImmutableList<Integer>> m_sortingColsSet;
  private final ImmutableSet<RelCollation> m_collationSet;

  public static final RelBucketing EMPTY =
      RelBucketingTraitDef.INSTANCE.canonize(
          new RelBucketingTraitImpl(ImmutableSet.of(ImmutableList.<Integer> of()), ImmutableSet
              .of(RelCollationImpl.EMPTY), null,
              null));

  public static RelBucketing of(
      ImmutableSet<ImmutableList<Integer>> partitioningColsSet,
      ImmutableSet<RelCollation> collationset, Integer noOfBuckets, Double szOfLargestBucket) {
    return new RelBucketingTraitImpl(partitioningColsSet, collationset, noOfBuckets,
        szOfLargestBucket);
  }

  public static RelBucketing of(List<Integer> partitioningCols, RelCollation collation,
      Integer noOfBuckets, Double szOfLargestBucket) {
    return new RelBucketingTraitImpl(ImmutableSet.<ImmutableList<Integer>> of(ImmutableList
        .<Integer> copyOf(partitioningCols)), ImmutableSet.<RelCollation> of(collation),
        noOfBuckets, szOfLargestBucket);
  }

  protected RelBucketingTraitImpl(ImmutableSet<ImmutableList<Integer>> partitioningColSet,
      ImmutableSet<RelCollation> collationset,
      Integer noOfBuckets, Double szOfLargestBucket) {
    m_partitioningColsSet = partitioningColSet;
    m_collationSet = collationset;
    m_noOfBuckets = noOfBuckets;
    m_szOfLargestBucket = szOfLargestBucket;
    m_sortingColsSet = getSetOfEquivalentSortCols(collationset);
  }

  @Override
  public RelTraitDef getTraitDef() {
    return RelBucketingTraitDef.INSTANCE;
  }

  @Override
  public boolean subsumes(RelTrait trait) {
    boolean subset = false;
    RelBucketing that = (RelBucketing) trait;

    if (equals(that) ) {
      if ((this.m_noOfBuckets == that.getNumberOfBuckets())
          && (this.m_szOfLargestBucket == that.getSizeOfLargestBucket())) {
        if (this.m_partitioningColsSet.containsAll(that.getPartitionCols())) {
          if ((this.m_collationSet == null && that.getCollation() == null)
              || ((this.m_collationSet != null && that.getCollation() != null) && this.m_collationSet
                  .containsAll(that.getCollation()))) {
            return true;
          }
        }
      }
    }

    return subset;
  }

  @Override
  public ImmutableSet<RelCollation> getCollation() {
    return m_collationSet;
  }

  @Override
  public int hashCode()
  {
    int hashCode = 1;

    hashCode = 31 * hashCode + m_partitioningColsSet.hashCode();
    hashCode = 31 * hashCode + m_collationSet.hashCode();
    hashCode = 31 * hashCode + m_noOfBuckets.hashCode();
    hashCode = 31 * hashCode + m_szOfLargestBucket.hashCode();

    return hashCode;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }

    if (obj instanceof RelBucketingTraitImpl) {
      RelBucketingTraitImpl that = (RelBucketingTraitImpl) obj;
      if ((this.m_noOfBuckets == that.m_noOfBuckets)
          && (this.m_szOfLargestBucket == that.m_szOfLargestBucket)) {
        if (this.m_partitioningColsSet.equals(that.m_partitioningColsSet)) {
          if ((this.m_collationSet == null && that.m_collationSet == null)
              || ((this.m_collationSet != null && that.m_collationSet != null) && this.m_collationSet
                  .equals(that.m_collationSet))) {
            return true;
          }
        }
      }
    }

    return false;
  }

  public boolean noOfBucketsMultipleOfEachOther(RelBucketing bucketTraitToCompare) {
    if (this == bucketTraitToCompare) {
      return true;
    }

    int largerNoOfBuckets = this.m_noOfBuckets;
    int smallererNoOfBuckets = bucketTraitToCompare.getNumberOfBuckets();
    if (this.m_noOfBuckets < bucketTraitToCompare.getNumberOfBuckets()) {
      largerNoOfBuckets = bucketTraitToCompare.getNumberOfBuckets();
      smallererNoOfBuckets = this.m_noOfBuckets;
    }

    if ((largerNoOfBuckets % smallererNoOfBuckets) == 0) {
      return true;
    }

    return false;
  }

  @Override
  public Integer getNumberOfBuckets() {
    return m_noOfBuckets;
  }

  @Override
  public Double getSizeOfLargestBucket() {
    return m_szOfLargestBucket;
  }

  @Override
  public ImmutableSet<ImmutableList<Integer>> getPartitionCols() {
    return m_partitioningColsSet;
  }

  @Override
  public ImmutableSet<ImmutableList<Integer>> getSortingCols() {
    return m_sortingColsSet;
  }

  @Override
  public Direction getNonNullSortOrder() {
    Direction nonNullSortOrder = null;
    if (m_sortingColsSet != null) {
      nonNullSortOrder = m_collationSet.asList().get(0).getFieldCollations().get(0).direction;
    }
    return nonNullSortOrder;
  }

  @Override
  public NullDirection getNullSortOrder() {
    NullDirection nullSortOrder = null;
    if (m_sortingColsSet != null) {
      nullSortOrder = m_collationSet.asList().get(0).getFieldCollations().get(0).nullDirection;
    }
    return nullSortOrder;
  }

  private static ImmutableSet<ImmutableList<Integer>> getSetOfEquivalentSortCols(
      ImmutableSet<RelCollation> collationset) {
    ImmutableSet<ImmutableList<Integer>> immutableSetOfSortCols = null;

    if (collationset != null && !collationset.isEmpty())
    {
      RelCollation representativeCollation = collationset.asList().get(0);
      int noOfSortingCols = representativeCollation.getFieldCollations().size();
      Direction bucketNonNullSortOrder = representativeCollation.getFieldCollations().get(0).direction;
      NullDirection bucketNullSortOrder = representativeCollation.getFieldCollations().get(0).nullDirection;

      Set<ImmutableList<Integer>> sortingColsSet = new HashSet<ImmutableList<Integer>>();
      List<Integer> sortCols = null;

      for (RelCollation collation : collationset) {
        if (collation.getFieldCollations().size() != noOfSortingCols) {
          throw new RuntimeException("Invalid Bucket Sort Order");
        }

        sortCols = new LinkedList<Integer>();
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
          sortCols.add(fieldCollation.getFieldIndex());
          if (bucketNonNullSortOrder != fieldCollation.direction
              || bucketNullSortOrder != fieldCollation.nullDirection) {
            throw new RuntimeException("Invalid Bucket Sort Order");
          }
        }

        if (!sortCols.isEmpty()) {
          sortingColsSet.add(ImmutableList.copyOf(sortCols));
        }
      }

      immutableSetOfSortCols = ImmutableSet.copyOf(sortingColsSet);
    }

    return immutableSetOfSortCols;
  }
}
