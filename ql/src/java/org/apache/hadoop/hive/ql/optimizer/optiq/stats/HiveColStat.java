package org.apache.hadoop.hive.ql.optimizer.optiq.stats;

public class HiveColStat {
    private final double m_avgSz;
    private final long m_ndv;
    
    public HiveColStat (double avgSz, long ndv) {
        m_avgSz = avgSz;
        m_ndv = ndv;
    }
    
    public double getAvgSz() {
        return m_avgSz;
    }
    
    public long getNDV() {
        return m_ndv;
    }
}
