package org.apache.hadoop.hive.ql.optimizer.optiq.cost;

public class HiveCosts {
	
	public static double PROCESSING_PER_UNIT = 0.01;
	public static double LOCAL_READ_PER_UNIT = 0.1;
	public static double LOCAL_WRITE_PER_UNIT = 0.2;
	public static double HDFS_READ_PER_UNIT = 0.3;
	public static double HDFS_WRITE_PER_UNIT = 0.4;

}
