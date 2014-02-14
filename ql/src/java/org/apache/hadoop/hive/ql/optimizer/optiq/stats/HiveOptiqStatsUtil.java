package org.apache.hadoop.hive.ql.optimizer.optiq.stats;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.RelOptHiveTable;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.Statistics;

/**
 * Utility for stats related stuff.<br>
 */

public class HiveOptiqStatsUtil {
  /*** Col Stats ***/

  public static List<HiveColStat> computeTableRelColStat(RelOptHiveTable hiveTbl,
      List<String> colNamesLst) {
    List<HiveColStat> hiveColStats = new LinkedList<HiveColStat>();

    Statistics stats = hiveTbl.getColumnStats(colNamesLst);
    if (stats.getColumnStats().size() != colNamesLst.size())
      throw new RuntimeException("Incomplete Col stats");

    for (ColStatistics colStat : stats.getColumnStats()) {
      hiveColStats.add(new HiveColStat((long) colStat.getAvgColLen(), colStat.getCountDistint()));
    }

    return hiveColStats;
  }
}
