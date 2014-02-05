package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveAggregateRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveLimitRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.EmptyRel;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.OneRowRelBase;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SetOpRel;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.rel.TableFunctionRelBase;
import org.eigenbase.rel.ValuesRelBase;
import org.eigenbase.rel.rules.MultiJoinRel;
import org.eigenbase.relopt.hep.HepRelVertex;
import org.eigenbase.relopt.volcano.RelSubset;

public class OptiqRelTreeIntroduceDerivedTables {

  public static void convertOpTree(RelNode rel, RelNode parent) {

    if (rel instanceof EmptyRel) {
      //TODO: replace with null scan
    } else if (rel instanceof HepRelVertex) {
    //TODO: is this relevant?
    } else if (rel instanceof HiveJoinRel) {
      if (!validJoinParent(rel, parent)) {
        introduceDerivedTable(rel, parent);
      }
    } else if (rel instanceof MultiJoinRel) {

    } else if (rel instanceof OneRowRelBase) {

    } else if (rel instanceof RelSubset) {

    } else if (rel instanceof SetOpRel) {

    } else if (rel instanceof SingleRel) {
    	if (rel instanceof FilterRelBase) {
    	      if (!validFilterParent(rel, parent)) {
    	          introduceDerivedTable(rel, parent);
    	        }    		
    	}else if (rel instanceof HiveLimitRel) {
  	      if (!validLimitParent(rel, parent)) {
	          introduceDerivedTable(rel, parent);
	        }    		    		
    	}else if (rel instanceof HiveAggregateRel) {
  	      if (!validGBParent(rel, parent)) {
	          introduceDerivedTable(rel, parent);
	        }    		    		
    	}
    } else if (rel instanceof TableAccessRelBase) {

    } else if (rel instanceof TableFunctionRelBase) {

    } else if (rel instanceof ValuesRelBase) {

    }

    List<RelNode> childNodes = rel.getInputs();
    if (childNodes != null) {
      for (RelNode r : childNodes) {
        convertOpTree(r, rel);
      }
    }
  }

  private static void introduceDerivedTable(RelNode rel, RelNode parent) {
    int i = 0;
    int pos = -1;
    List<RelNode> childList = parent.getInputs();

    for (RelNode child : childList) {
      if (child == rel) {
        pos = i;
        break;
      }
      i++;
    }

    if (pos == -1) {
      throw new RuntimeException("Couldn't find child node in parent's inputs");
    }

    HiveProjectRel select = HiveProjectRel.create(rel.getCluster(), rel, rel.getChildExps(), rel.getRowType(),
        rel.getCollationList());
    parent.replaceInput(pos, select);

  }

  private static boolean validJoinParent(RelNode joinNode, RelNode parent) {
    boolean validParent = true;

    if (parent instanceof JoinRelBase) {
      if (((JoinRelBase) parent).getRight() == joinNode) {
        validParent = false;
      }
    } else if (parent instanceof SetOpRel) {
      validParent = false;
    }

    return validParent;
  }

  private static boolean validFilterParent(RelNode filterNode, RelNode parent) {
    boolean validParent = true;

    //TOODO: Verify GB having is not a seperate filter (if so we shouldn't introduce derived table)
    if (parent instanceof FilterRelBase || parent instanceof JoinRelBase || parent instanceof SetOpRel) {
        validParent = false;
      }

    return validParent;
  }
  
  private static boolean validGBParent(RelNode gbNode, RelNode parent) {
	    boolean validParent = true;

	    //TOODO: Verify GB having is not a seperate filter (if so we shouldn't introduce derived table)
	    if (parent instanceof JoinRelBase || parent instanceof SetOpRel || parent instanceof AggregateRelBase) {
	        validParent = false;
	      }

	    return validParent;
	  }
  
  private static boolean validLimitParent(RelNode gbNode, RelNode parent) {
	    boolean validParent = true;

	    if (!(parent instanceof ProjectRelBase)) {
	        validParent = false;
	      }

	    return validParent;
	  }
}
