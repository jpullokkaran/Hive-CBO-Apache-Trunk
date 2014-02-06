package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.hydromatic.optiq.util.BitSets;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveAggregateRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveIRBroadCastRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveIRShuffleRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.MapJoinStreamingRelation;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveLimitRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.volcano.RelSubset;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.eigenbase.util.Util;

public class OptiqUtil {
	public static <T> boolean orderedSubset(List<T> list1, List<T> list2) {
    return Util.startsWith(list1, list2);
	}

	public static <T> boolean orderedSubset(
			ImmutableSet<ImmutableList<T>> setOfList1, List<T> list2) {
		for (List<T> lst : setOfList1) {
			if (orderedSubset(lst, list2)) {
				return true;
			}
		}

		return false;
	}

  public static JoinPredicateInfo create(HiveJoinRel j) {
    List<RexNode> leftJoinKeys = new LinkedList<RexNode>();
    List<RexNode> rightJoinKeys = new LinkedList<RexNode>();
    List<Integer> filterNulls = new LinkedList<Integer>();

    RexNode remainingNonJoinConjunctivePredicate =
        RelOptUtil.splitJoinCondition(j.getSystemFieldList(), j.getLeft(),
            j.getRight(), j.getCondition(), leftJoinKeys, rightJoinKeys,
            filterNulls, null);

    List<Integer> joinKeysFromLeftRelations = getIndexList(leftJoinKeys);
    List<Integer> joinKeysFromRightRelations = getIndexList(rightJoinKeys);

    int leftFieldCount = j.getLeft().getRowType().getFieldCount();
    return new JoinPredicateInfo(joinKeysFromLeftRelations,
        joinKeysFromRightRelations, leftFieldCount,
        RelOptUtil.conjunctions(remainingNonJoinConjunctivePredicate));
  }

  // TODO: Is this needed (Could RelSubSet ever be child of a node?)
	public static HiveRel getNonSubsetRelNode(RelNode n) {
    if (false) throw new AssertionError(); // THIS METHOD IS EVIL
		if (n instanceof RelSubset) {
			return ((HiveRel) ((RelSubset) n).getBest());
		} else {
			return ((HiveRel) n);
		}
	}

	public static Map<Integer, Integer> translateChildColPosToParent(
			HiveJoinRel j, boolean translateLeft) {
		HiveRel childToTranslateFrom;
		final int colPosOffSet;

		if (translateLeft) {
			childToTranslateFrom = getNonSubsetRelNode(j.getLeft());
      colPosOffSet = 0;
		} else {
			childToTranslateFrom = getNonSubsetRelNode(j.getRight());
			colPosOffSet = j.getLeft().getRowType().getFieldCount();
		}
    Map<Integer, Integer> projMapFromChildToParent = new HashMap<Integer, Integer>();
		for (int i = 0; i < childToTranslateFrom.getRowType().getFieldCount(); i++) {
			projMapFromChildToParent.put(i, i + colPosOffSet);
		}

		return projMapFromChildToParent;
	}

	public static Map<Integer, Integer> translateChildColPosToParent(
			List<RexNode> exps) {
		Map<Integer, Integer> projMapFromChildToParent = new HashMap<Integer, Integer>();
		for (int i = 0; i < exps.size(); i++) {
			if (exps.get(i) instanceof RexInputRef) {
				projMapFromChildToParent.put(
						((RexInputRef) exps.get(i)).getIndex(), i);
			}
		}
		return projMapFromChildToParent;
	}

  // Never returns null, but some entries may be null.
	public static List<Integer> translateChildColPosToParent(
			Map<Integer, Integer> projMapFromChildToParent,
			List<Integer> childPosLst, boolean allowForPartialTranslation) {
		List<Integer> translatedPos = new LinkedList<Integer>();

		for (Integer childPos : childPosLst) {
      Integer posInParent = projMapFromChildToParent.get(childPos);
			if (posInParent == null) {
				if (!allowForPartialTranslation) {
					translatedPos = null;
				}
				break;
			}

			translatedPos.add(posInParent);
		}

		return translatedPos;
	}

  // REVIEW: Why Set<ImmutableList<Integer> not Set<List<Integer>>?
  //   Over-specific element type makes it more difficult to use widely
	public static ImmutableSet<ImmutableList<Integer>> translateChildColPosToParent(
			Map<Integer, Integer> projMapFromChildToParent,
			ImmutableSet<ImmutableList<Integer>> childPosListImmutableSet,
			boolean allowForPartialTranslation) {
		Set<ImmutableList<Integer>> childPosListMutableSet =
        new HashSet<ImmutableList<Integer>>();

		for (List<Integer> childPosList : childPosListImmutableSet) {
      List<Integer> translatedPos = translateChildColPosToParent(
					projMapFromChildToParent, childPosList,
					allowForPartialTranslation);
			if (translatedPos != null && !translatedPos.isEmpty()) {
				childPosListMutableSet.add(ImmutableList.copyOf(translatedPos));
			}
		}

    if (childPosListMutableSet.isEmpty()) {
      // REVIEW: Why return null rather than empty set?
      return null;
    }
    return ImmutableSet.copyOf(childPosListMutableSet);
  }

	// projIndxInParentLst can not contain any Virtual Col
	public static Pair<List<Integer>, List<Integer>> translateProjIndxToChild(
			HiveJoinRel parentJoin, List<Integer> projIndxInParentLst) {
		if (projIndxInParentLst == null) {
			return null;
		}

    // REVIEW: Why LinkedList? ArrayList (or ImmutableList.Builder) more
    //   efficient
		List<Integer> leftChildProj = new LinkedList<Integer>();
		List<Integer> rightChildProj = new LinkedList<Integer>();

		int rightOffSet = parentJoin.getLeft().getRowType().getFieldCount();
		for (Integer projIndx : projIndxInParentLst) {
			if (projIndx < rightOffSet) {
				leftChildProj.add(projIndx);
			} else {
				rightChildProj.add(projIndx - rightOffSet);
			}
		}

		return new Pair<List<Integer>, List<Integer>>(leftChildProj,
				rightChildProj);
	}

	// projIndxInParentLst can not contain any Virtual Col
	public static List<Integer> translateProjIndxToChild(HiveRel parentRel,
			List<Integer> projIndxInParentLst) {
		List<Integer> childProj = new LinkedList<Integer>();
		try {
			for (Integer projIndx : projIndxInParentLst) {
				RexNode field = parentRel.getChildExps().get(projIndx);
				childProj.add(((RexInputRef) field).getIndex());
			}
		} catch (Exception e) {
			// TODO Log here
      childProj = null;
      throw new AssertionError(e);
		}
		return childProj;
	}

	public static List<Integer> translateBitSetToProjIndx(BitSet projBitSet) {
    return BitSets.toList(projBitSet);
	}

	private static List<Integer> getIndexList(
			List<RexNode> lstOfJoinKeysFromChildRel) {
    todo("move to RelOptUtil");
		List<Integer> indexLst = new LinkedList<Integer>();

		for (RexNode r : lstOfJoinKeysFromChildRel) {
			if (r instanceof RexInputRef) {
				indexLst.add(((RexInputRef) r).getIndex());
			} else {
				// TODO: validate this
				throw new RuntimeException("Invalid rex node");
			}
		}

		return indexLst;
	}

  @Deprecated
  private static void todo(String s) {
  }

  /** Information about the condition of a
   * {@link org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel}.
   *
   * <p>None of the fields are ever null. */
  public static class JoinPredicateInfo {
		private final List<RexNode> m_nonJoinKeyLeafPredicates;
		private final List<Integer> m_joinKeysFromLeftRelations;
		private final List<Integer> m_joinKeysFromRightRelations;
		private final List<Integer> m_joinKeysInJoinNodeSchemaFromLeft;
		private final List<Integer> m_joinKeysInJoinNodeSchemaFromRight;

    public JoinPredicateInfo(List<Integer> joinKeysFromLeftRelations,
        List<Integer> joinKeysFromRightRelations, int leftFieldCount,
        List<RexNode> conjunctions) {
      this.m_joinKeysFromLeftRelations =
          ImmutableList.copyOf(joinKeysFromLeftRelations);
      this.m_joinKeysFromRightRelations =
          ImmutableList.copyOf(joinKeysFromRightRelations);
      this.m_joinKeysInJoinNodeSchemaFromLeft = m_joinKeysFromLeftRelations;
      ImmutableList.Builder<Integer> builder = ImmutableList.builder();
      for (int k : m_joinKeysFromRightRelations) {
        builder.add(leftFieldCount + k);
      }
      this.m_joinKeysInJoinNodeSchemaFromRight = builder.build();
      this.m_nonJoinKeyLeafPredicates = ImmutableList.copyOf(conjunctions);
    }

		public boolean containsVirtualColumns(RelNode rel, Set<Integer> colSet) {
			boolean containsVC = false;

			List<RelDataTypeField> dtLst = rel.getRowType().getFieldList();
			for (Integer col : colSet) {
				if (!(dtLst.get(col) instanceof RexInputRef)) {
					containsVC = true;
					break;
				}
			}

			return containsVC;
		}

    public List<Integer> getJoinKeysInJoinSchemaFromLeft() {
			return m_joinKeysInJoinNodeSchemaFromLeft;
		}

		public List<Integer> getJoinKeysInJoinSchemaFromRight() {
			return m_joinKeysInJoinNodeSchemaFromRight;
		}

    // Never returns null
		public List<RexNode> getNonJoinKeyLeafPredicates() {
			return m_nonJoinKeyLeafPredicates;
		}

		public List<Integer> getJoinKeysFromLeftRelation() {
			return m_joinKeysFromLeftRelations;
		}

		public List<Integer> getJoinKeysFromRightRelation() {
			return m_joinKeysFromRightRelations;
		}
	}

	public static HiveJoinRel introduceShuffleOperator(HiveJoinRel origJoin,
			boolean introduceShuffleAtLeft, boolean introduceShuffleAtRight,
			List<Integer> leftSortKeys, List<Integer> rightSortKeys, RelOptPlanner relOptPlanner) {
		HiveRel leftOfNewJoin = (HiveRel) getNonSubsetRelNode(origJoin
				.getLeft());
		HiveRel rightOfNewJoin = (HiveRel) getNonSubsetRelNode(origJoin
				.getRight());
		HiveRel oldRel;

    // REVIEW: should use convert
		if (introduceShuffleAtLeft) {
			oldRel =  leftOfNewJoin;
			leftOfNewJoin = HiveIRShuffleRel.constructHiveIRShuffleRel(
					leftOfNewJoin.getCluster(), leftOfNewJoin.getTraitSet(),
					leftSortKeys, leftOfNewJoin);
			relOptPlanner.ensureRegistered(leftOfNewJoin, oldRel);
		}

    // REVIEW: should use convert
		if (introduceShuffleAtRight) {
			oldRel =  rightOfNewJoin;
			rightOfNewJoin = HiveIRShuffleRel.constructHiveIRShuffleRel(
					rightOfNewJoin.getCluster(), rightOfNewJoin.getTraitSet(),
					rightSortKeys, rightOfNewJoin);
			relOptPlanner.ensureRegistered(rightOfNewJoin, oldRel);
		}

		HiveJoinRel newJoin = origJoin.copy(origJoin.getTraitSet(),
				origJoin.getCondition(), leftOfNewJoin, rightOfNewJoin,
				JoinAlgorithm.COMMON_JOIN, MapJoinStreamingRelation.NONE);

		return newJoin;
	}

	public static HiveJoinRel introduceBroadcastOperator(HiveJoinRel origJoin,
			MapJoinStreamingRelation m_streamingSide) {
		HiveRel leftOfNewJoin = getNonSubsetRelNode(origJoin.getLeft());
		HiveRel rightOfNewJoin = getNonSubsetRelNode(origJoin.getRight());

		if (m_streamingSide == MapJoinStreamingRelation.LEFT_RELATION) {
			rightOfNewJoin = HiveIRBroadCastRel.constructHiveIRBroadCastRel(
					rightOfNewJoin.getCluster(), rightOfNewJoin.getTraitSet(),
					rightOfNewJoin);
		} else {
			leftOfNewJoin = HiveIRBroadCastRel.constructHiveIRBroadCastRel(
					leftOfNewJoin.getCluster(), leftOfNewJoin.getTraitSet(),
					leftOfNewJoin);
		}

		HiveJoinRel newJoin = origJoin.copy(origJoin.getTraitSet(),
				origJoin.getCondition(), leftOfNewJoin, rightOfNewJoin,
				JoinAlgorithm.MAP_JOIN, m_streamingSide);

		return newJoin;
	}

	public static RelNode createProjectIfNeeded(final HiveRel child,
			final List<Integer> posList) {
		if (isIdentity(posList, child.getRowType().getFieldCount())) {
			return child;
		} else {
			List<RexNode> inputRefs = constructInputRefs(child, posList);
			return HiveProjectRel.create(child, inputRefs, null);
		}
	}

	public static RelNode createProjectIfNeeded(final HiveRel child,
			List<RexNode> exps, List<String> fieldNames) {
		final RelOptCluster cluster = child.getCluster();
		final RelDataType rowType = RexUtil.createStructType(
				cluster.getTypeFactory(), exps, fieldNames);
		if (RemoveTrivialProjectRule.isIdentity(exps, rowType,
				child.getRowType())) {
			return child;
		}
		return HiveProjectRel.create(child, exps, null);
	}

	public static List<RexNode> constructChildInputRefs(HiveRel child) {
		return constructInputRefs(child, constructProjIndxLst(child));
	}

	private static List<RexNode> constructInputRefs(final HiveRel child,
			List<Integer> projPosLst) {
		RexNode inputRef;
		final List<RexNode> projExps = new LinkedList<RexNode>();

		for (Integer pos : projPosLst) {
			inputRef = child
					.getCluster()
					.getRexBuilder()
					.makeInputRef(
							child.getRowType().getFieldList().get(pos)
									.getType(), pos);
			projExps.add(inputRef);
		}

		return projExps;
	}

	public static List<Integer> constructProjIndxLst(HiveRel relNode) {
		List<Integer> projIndxLst = new LinkedList<Integer>();
		int noOfProjs = relNode.getRowType().getFieldCount();

		for (Integer i = 0; i < noOfProjs; i++)
			projIndxLst.add(i);

		return projIndxLst;
	}

	private static boolean isIdentity(List<Integer> list, int count) {
		if (list.size() != count) {
			return false;
		}
		for (int i = 0; i < count; i++) {
			final Integer o = list.get(i);
			if (o == null || o != i) {
				return false;
			}
		}
		return true;
	}

	public static boolean isAlreadyStreamingWithinSameVertex(HiveRel n) {
		if (n instanceof HiveProjectRel || n instanceof HiveFilterRel
				|| n instanceof HiveLimitRel) {
			return isAlreadyStreamingWithinSameVertex((HiveRel) n.getInput(0));
		} else if (n instanceof HiveJoinRel || n instanceof HiveAggregateRel) {
			return true;
		}

		return false;
	}

	public static List<Integer> getVirtualCols(List<RexNode> exps) {
		ImmutableList.Builder<Integer> vCols = ImmutableList.builder();

		for (int i = 0; i < exps.size(); i++) {
			if (!(exps.get(i) instanceof RexInputRef)) {
				vCols.add(i);
			}
		}

		return vCols.build();
	}

	private static boolean getEqvSet(Set<Integer> eqvSet,
			Map<Integer, Set<Integer>> projIndxToEquivalencyMap,
			List<RexNode> keys, int projOffset) {
		Set<Integer> curEqv = null;
		int projPos = 0;
		boolean oneSideIsLiteral = false;

		for (RexNode jk : keys) {
			curEqv = null;
			// TODO: Literals and functions needs to be handled
			if (jk instanceof RexLiteral) {
				oneSideIsLiteral = true;
				continue;
			} else if (!(jk instanceof RexInputRef)) {
				continue;
			}
			projPos = ((RexInputRef) jk).getIndex() + projOffset;
			eqvSet.add(projPos);
		}

		return oneSideIsLiteral;
	}
/*
	private static void getNDV(RelNode leftRel, RelNode rightRel,
			RexNode condition) {
		List<RelDataTypeField> sysFieldList = new LinkedList<RelDataTypeField>();
		List<RexNode> leftJoinKeys = new LinkedList<RexNode>();
		List<RexNode> rightJoinKeys = new LinkedList<RexNode>();
		List<Integer> filterNulls = new LinkedList<Integer>();
		List<SqlOperator> rangeOp = new LinkedList<SqlOperator>();
		List<RexNode> nonEquiList = new LinkedList<RexNode>();
		List<RexNode> decomposedConjLst = new ArrayList<RexNode>();
		RexNode remainingNonJoinConjuctivePredicate = null;
		Map<Integer, Set<Integer>> projIndxToEquivalencyMap = new HashMap<Integer, Set<Integer>>();
		Set<Integer> curEqv = null;
		Set<Integer> eqvSet = new HashSet<Integer>();
		Boolean literalComparison = false;
		boolean leftContainsLiteral = false;
		boolean rightContainLiteral = false;

		int rightProjOffSet = leftRel.getRowType().getFieldCount();

		// 1. Decompose Predicate in to list of conjuctive elements
		RelOptUtil.decomposeConjunction(condition, decomposedConjLst);

		// 2. Get Join Keys from left and right for each leaf predicate
		for (RexNode leafPredicate : decomposedConjLst) {
			// 2.1 clear out all list of prev state
			sysFieldList.clear();
			leftJoinKeys.clear();
			rightJoinKeys.clear();
			filterNulls.clear();
			rangeOp.clear();
			nonEquiList.clear();
			eqvSet.clear();

			// 2.2 Split leaf predicates of Join condition to elements from left
			// and right
			// TODO: Pay attention to functions on either side
			remainingNonJoinConjuctivePredicate = RelOptUtil
					.splitJoinCondition(sysFieldList, leftRel, rightRel,
							leafPredicate, leftJoinKeys, rightJoinKeys,
							filterNulls, null);

			// 2.3 Add ProjPos from left to EQV set (Proj Pos in the output join
			// schema)
			literalEqv = getEqvSet(eqvSet, projIndxToEquivalencyMap, leftJoinKeys, 0);
			getEqvSet(eqvSet, projIndxToEquivalencyMap, rightJoinKeys,
					rightProjOffSet);

			// 2.4 Build up Transitive Equivalency Set
			Set<Integer> transitiveEqvSet = new HashSet<Integer>(eqvSet);
			for (Integer eqvProjPos : eqvSet) {
				curEqv = projIndxToEquivalencyMap.get(eqvProjPos);
				if (curEqv != null)
					transitiveEqvSet.addAll(curEqv);
			}

			// 2.5 Update the map to point to the new Transitive EQV Set
			for (Integer eqvProjPos : transitiveEqvSet) {
				projIndxToEquivalencyMap.put(eqvProjPos, transitiveEqvSet);
			}
		}
	}
	*/ 
}
