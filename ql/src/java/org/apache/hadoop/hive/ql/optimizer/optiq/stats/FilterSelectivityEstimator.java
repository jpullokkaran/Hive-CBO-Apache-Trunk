package org.apache.hadoop.hive.ql.optimizer.optiq.stats;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptUtil.InputReferencedVisitor;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.SqlKind;

public class FilterSelectivityEstimator extends RexVisitorImpl<Double> {
  private final RelNode m_childRel;
  private final double  m_childCardinality;

  protected FilterSelectivityEstimator(RelNode childRel) {
    super(true);
    m_childRel = childRel;
    m_childCardinality = RelMetadataQuery.getRowCount(m_childRel);
  }

  public Double estimateSelectivity(RexNode predicate) {
    return predicate.accept(this);
  }

  public Double visitCall(RexCall call) {
    if (!deep) {
      return 1.0;
    }

    Double selectivity = null;
    SqlKind op = call.getKind();

    switch (op) {
    case AND: {
      selectivity = computeConjunctionSelectivity(call);
      break;
    }

    case OR: {
      selectivity = computeDisjunctionSelectivity(call);
      break;
    }

    case NOT_EQUALS: {
      selectivity = computeNotEqualitySelectivity(call);
    }

    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case GREATER_THAN: {
      selectivity = ((double) 1 / (double) 3);
      break;
    }

    case IN: {
      selectivity = ((double) 1 / ((double) call.operands.size()));
      break;
    }

    default:
      selectivity = computeFunctionSelectivity(call);
    }

    return selectivity;
  }

  /**
   * NDV of "f1(x, y, z) != f2(p, q, r)" ->
   * "(maxNDV(x,y,z,p,q,r) - 1)/maxNDV(x,y,z,p,q,r)".
   * <p>
   * 
   * @param call
   * @return
   */
  private Double computeNotEqualitySelectivity(RexCall call) {
    double tmpNDV = getMaxNDV(call);

    if (tmpNDV > 1)
      return (tmpNDV - (double) 1) / tmpNDV;
    else
      return 1.0;
  }

  /**
   * Selectivity of f(X,y,z) -> 1/maxNDV(x,y,z).
   * <p>
   * Note that >, >=, <, <=, = ... are considered generic functions and uses
   * this method to find their selectivity.
   * 
   * @param call
   * @return
   */
  private Double computeFunctionSelectivity(RexCall call) {
    return 1 / getMaxNDV(call);
  }

  /**
   * Disjunction Selectivity -> (1 �(1-m1/n)(1-m2/n)) where n is the total
   * number of tuples from child and m1 and m2 is the expected number of tuples
   * from each part of the disjunction predicate.
   * <p>
   * Note we compute m1. m2.. by applying selectivity of the disjunctive element
   * on the cardinality from child.
   * 
   * @param call
   * @return
   */
  private Double computeDisjunctionSelectivity(RexCall call) {
    Double tmpCardinality;
    Double tmpSelectivity;
    double selectivity = 1;

    for (RexNode dje : call.getOperands()) {
      tmpSelectivity = dje.accept(this);
      if (tmpSelectivity == null) {
        tmpSelectivity = 0.99;
      }
      tmpCardinality = m_childCardinality * tmpSelectivity;

      if (tmpCardinality > 1)
        tmpSelectivity = (1 - tmpCardinality / m_childCardinality);
      else
        tmpSelectivity = 1.0;

      selectivity *= tmpSelectivity;
    }

    if (selectivity > 1)
      return (1 - selectivity);
    else
      return 1.0;
  }

  /**
   * Selectivity of conjunctive predicate -> (selectivity of conjunctive
   * element1) * (selectivity of conjunctive element2)...
   * 
   * @param call
   * @return
   */
  private Double computeConjunctionSelectivity(RexCall call) {
    Double tmpSelectivity;
    double selectivity = 1;

    for (RexNode cje : call.getOperands()) {
      tmpSelectivity = cje.accept(this);
      if (tmpSelectivity != null) {
        selectivity *= tmpSelectivity;
      }
    }

    return selectivity;
  }

  private Double getMaxNDV(RexCall call) {
    double tmpNDV;
    double maxNDV = 1.0;
    InputReferencedVisitor irv;

    for (RexNode op : call.getOperands()) {
      if (op instanceof RexInputRef) {
        tmpNDV = HiveRelMdDistinctRowCount.getDistinctRowCount(m_childRel,
            ((RexInputRef) op).getIndex());
        if (tmpNDV > maxNDV)
          maxNDV = tmpNDV;
      } else {
        irv = new InputReferencedVisitor();
        irv.apply(op);
        for (Integer childProjIndx : irv.inputPosReferenced) {
          tmpNDV = HiveRelMdDistinctRowCount.getDistinctRowCount(m_childRel, childProjIndx);
          if (tmpNDV > maxNDV)
            maxNDV = tmpNDV;
        }
      }
    }

    return maxNDV;
  }
}
