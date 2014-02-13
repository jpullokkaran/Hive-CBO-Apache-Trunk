package org.apache.hadoop.hive.ql.optimizer.optiq;

import com.google.common.collect.ImmutableList;
import org.eigenbase.rel.metadata.ChainedRelMetadataProvider;
import org.eigenbase.rel.metadata.DefaultRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataProvider;

public class HiveDefaultRelMetadataProvider {
  private HiveDefaultRelMetadataProvider() {}

  public static final RelMetadataProvider INSTANCE =
      ChainedRelMetadataProvider.of(
          ImmutableList.of(
              HiveRelMdDistinctRowCount.SOURCE,
              HiveRelMdSelectivity.SOURCE,
              new DefaultRelMetadataProvider()));
}
