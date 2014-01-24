package org.apache.hadoop.hive.ql.optimizer.optiq.cost;

import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptUtil;

// TODO: This should inherit from VolcanoCost and should just override isLE method.
public class HiveCost implements RelOptCost {
  // ~ Static fields/initializers ---------------------------------------------

  static final HiveCost INFINITY =
      new HiveCost(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY)
      {
        @Override
        public String toString()
        {
          return "{inf}";
        }
      };

  static final HiveCost HUGE =
      new HiveCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
        @Override
        public String toString()
        {
          return "{huge}";
        }
      };

  static final HiveCost ZERO =
      new HiveCost(0.0, 0.0, 0.0) {
        @Override
        public String toString()
        {
          return "{0}";
        }
      };

  static final HiveCost TINY =
      new HiveCost(1.0, 1.0, 0.0) {
        @Override
        public String toString()
        {
          return "{tiny}";
        }
      };

  // ~ Instance fields --------------------------------------------------------

  double dCpu;
  double dIo;
  double dRows;

  // ~ Constructors -----------------------------------------------------------

  HiveCost(
      double dRows,
      double dCpu,
      double dIo)
  {
    set(dRows, dCpu, dIo);
  }

  // ~ Methods ----------------------------------------------------------------

  public double getCpu()
  {
    return dCpu;
  }

  public boolean isInfinite()
  {
    return (this == INFINITY) || (this.dRows == Double.POSITIVE_INFINITY)
        || (this.dCpu == Double.POSITIVE_INFINITY)
        || (this.dIo == Double.POSITIVE_INFINITY);
  }

  public double getIo()
  {
    return dIo;
  }

  // TODO: If two cost is equal, could we do any better than comparing cardinality (may be some
  // other heuristics to break the tie)
  public boolean isLe(RelOptCost other) {
    if (((this.dCpu + this.dIo) < (other.getCpu() + other.getIo()))
        || ((this.dCpu + this.dIo) == (other.getCpu() + other.getIo()) && this.dRows <= other.getRows())) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isLt(RelOptCost other)
  {
    return isLe(other) && !equals(other);
  }

  public double getRows()
  {
    return dRows;
  }

  public boolean equals(RelOptCost other)
  {
    //TODO: should we consider cardinality as well?
    return (this == other)
        || ((this.dCpu + this.dIo) ==  (other.getCpu() + other.getIo()));
  }

  public boolean isEqWithEpsilon(RelOptCost other)
  {
    return (this == other)
        || (Math.abs((this.dCpu + this.dIo) - (other.getCpu() + other.getIo())) < RelOptUtil.EPSILON);
  }

  public RelOptCost minus(RelOptCost other)
  {
    if (this == INFINITY) {
      return this;
    }

    return new HiveCost(
        this.dRows - other.getRows(),
        this.dCpu - other.getCpu(),
        this.dIo - other.getIo());
  }

  public RelOptCost multiplyBy(double factor)
  {
    if (this == INFINITY) {
      return this;
    }
    return new HiveCost(dRows * factor, dCpu * factor, dIo * factor);
  }

  public double divideBy(RelOptCost cost)
  {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    double d = 1;
    double n = 0;
    if ((this.dRows != 0)
        && !Double.isInfinite(this.dRows)
        && (cost.getRows() != 0)
        && !Double.isInfinite(cost.getRows()))
    {
      d *= this.dRows / cost.getRows();
      ++n;
    }
    if ((this.dCpu != 0)
        && !Double.isInfinite(this.dCpu)
        && (cost.getCpu() != 0)
        && !Double.isInfinite(cost.getCpu()))
    {
      d *= this.dCpu / cost.getCpu();
      ++n;
    }
    if ((this.dIo != 0)
        && !Double.isInfinite(this.dIo)
        && (cost.getIo() != 0)
        && !Double.isInfinite(cost.getIo()))
    {
      d *= this.dIo / cost.getIo();
      ++n;
    }
    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  public RelOptCost plus(RelOptCost other)
  {
    if ((this == INFINITY) || (other.isInfinite())) {
      return INFINITY;
    }
    return new HiveCost(
        this.dRows + other.getRows(),
        this.dCpu + other.getCpu(),
        this.dIo + other.getIo());
  }

  public void set(
      double dRows,
      double dCpu,
      double dIo)
  {
    this.dRows = dRows;
    this.dCpu = dCpu;
    this.dIo = dIo;
  }

  @Override
  public String toString()
  {
    return "{" + dRows + " rows, " + dCpu + " cpu, " + dIo + " io}";
  }
}
