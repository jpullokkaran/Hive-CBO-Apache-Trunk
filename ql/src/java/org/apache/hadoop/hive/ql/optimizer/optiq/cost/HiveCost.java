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
    HiveCost that = (HiveCost) other;
    if (((this.dCpu + this.dIo) < (that.dCpu + that.dIo))
        || ((this.dCpu + this.dIo) == (that.dCpu + that.dIo) && this.dRows <= that.dRows)) {
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
    if (!(other instanceof HiveCost)) {
      return false;
    }
    HiveCost that = (HiveCost) other;
    //TODO: should we consider cardinality as well?
    return (this == that)
        || ((this.dCpu + this.dIo) ==  (that.dCpu + that.dIo));
  }

  public boolean isEqWithEpsilon(RelOptCost other)
  {
    if (!(other instanceof HiveCost)) {
      return false;
    }
    HiveCost that = (HiveCost) other;
    return (this == that)
        || (Math.abs((this.dCpu + this.dIo) - (that.dCpu + that.dIo)) < RelOptUtil.EPSILON);
  }

  public RelOptCost minus(RelOptCost other)
  {
    if (this == INFINITY) {
      return this;
    }
    HiveCost that = (HiveCost) other;
    return new HiveCost(
        this.dRows - that.dRows,
        this.dCpu - that.dCpu,
        this.dIo - that.dIo);
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
    HiveCost that = (HiveCost) cost;
    double d = 1;
    double n = 0;
    if ((this.dRows != 0)
        && !Double.isInfinite(this.dRows)
        && (that.dRows != 0)
        && !Double.isInfinite(that.dRows))
    {
      d *= this.dRows / that.dRows;
      ++n;
    }
    if ((this.dCpu != 0)
        && !Double.isInfinite(this.dCpu)
        && (that.dCpu != 0)
        && !Double.isInfinite(that.dCpu))
    {
      d *= this.dCpu / that.dCpu;
      ++n;
    }
    if ((this.dIo != 0)
        && !Double.isInfinite(this.dIo)
        && (that.dIo != 0)
        && !Double.isInfinite(that.dIo))
    {
      d *= this.dIo / that.dIo;
      ++n;
    }
    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  public RelOptCost plus(RelOptCost other)
  {
    HiveCost that = (HiveCost) other;
    if ((this == INFINITY) || (that == INFINITY)) {
      return INFINITY;
    }
    return new HiveCost(
        this.dRows + that.dRows,
        this.dCpu + that.dCpu,
        this.dIo + that.dIo);
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
