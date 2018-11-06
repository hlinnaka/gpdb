/*-------------------------------------------------------------------------
 *
 * cdbpathlocus.c
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbpathlocus.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/gp_policy.h"	/* GpPolicy */
#include "cdb/cdbdef.h"			/* CdbSwap() */
#include "cdb/cdbpullup.h"		/* cdbpullup_findDistributionKeyExprInTargetList() */
#include "nodes/makefuncs.h"	/* makeVar() */
#include "nodes/nodeFuncs.h"	/* exprType() and exprTypmod() */
#include "nodes/plannodes.h"	/* Plan */
#include "nodes/relation.h"		/* RelOptInfo */
#include "optimizer/pathnode.h" /* Path */
#include "optimizer/paths.h"	/* cdb_make_distkey_for_expr() */
#include "optimizer/tlist.h"	/* tlist_member() */

#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbpathlocus.h"	/* me */

static List *cdb_build_distribution_keys(PlannerInfo *root,
								RelOptInfo *rel,
								int nattrs,
								AttrNumber *attrs);

/*
 * cdbpathlocus_equal
 *
 *    - Returns false if locus a or locus b is "strewn", meaning that it
 *      cannot be determined whether it is equal to another partitioned
 *      distribution.
 *
 *    - Returns true if a and b have the same 'locustype' and 'distkey'.
 *
 *    - Returns true if both a and b are hashed and the set of possible
 *      m-tuples of expressions (e1, e2, ..., em) produced by a's partkey
 *      is equal to the set produced by b's distkey.
 *
 *    - Returns false otherwise.
 */
bool
cdbpathlocus_equal(CdbPathLocus a, CdbPathLocus b)
{
	ListCell   *acell;
	ListCell   *bcell;

	/* Unless a and b have the same numsegments the result is always false */
	if (CdbPathLocus_NumSegments(a) !=
		CdbPathLocus_NumSegments(b))
		return false;

	if (CdbPathLocus_IsStrewn(a) ||
		CdbPathLocus_IsStrewn(b))
		return false;

	if (CdbPathLocus_IsEqual(a, b))
		return true;

	if (CdbPathLocus_Degree(a) == 0 ||
		CdbPathLocus_Degree(b) == 0 ||
		CdbPathLocus_Degree(a) != CdbPathLocus_Degree(b))
		return false;

	if (a.locustype == b.locustype)
	{
		if (CdbPathLocus_IsHashed(a))
		{
			forboth(acell, a.distkeys, bcell, b.distkeys)
			{
				DistributionKey *adistkey = (DistributionKey *) lfirst(acell);
				DistributionKey *bdistkey = (DistributionKey *) lfirst(bcell);

				if (adistkey->dk_eclass != bdistkey->dk_eclass)
					return false;
			}
			return true;
		}
	}

	Assert(false);
	return false;
}								/* cdbpathlocus_equal */

/*
 * cdb_build_distribution_keys
 *	  Build canonicalized pathkeys list for given columns of rel.
 *
 *    Returns a List, of length 'nattrs': each of its members is
 *    a List of one or more DistributionKey nodes.  The returned List
 *    might contain duplicate entries: this occurs when the
 *    corresponding columns are constrained to be equal.
 *
 *    The caller receives ownership of the returned List, freshly
 *    palloc'ed in the caller's context.  The members of the returned
 *    List are shared and might belong to the caller's context or
 *    other contexts.
 */
static List *
cdb_build_distribution_keys(PlannerInfo *root,
							RelOptInfo *rel,
							int nattrs,
							AttrNumber *attrs)
{
	List	   *retval = NIL;
	List	   *eq = list_make1(makeString("="));
	int			i;

	for (i = 0; i < nattrs; ++i)
	{
		DistributionKey *cdistkey;

		/* Find or create a Var node that references the specified column. */
		Var		   *expr = find_indexkey_var(root, rel, attrs[i]);

		/*
		 * Find or create a distribution key.
		 */
		cdistkey = cdb_make_distkey_for_expr(root, (Node *) expr, eq);

		/* Append to list of distribution keys. */
		retval = lappend(retval, cdistkey);
	}

	list_free_deep(eq);
	return retval;
}

/*
 * cdbpathlocus_from_baserel
 *
 * Returns a locus describing the distribution of a base relation.
 */
CdbPathLocus
cdbpathlocus_from_baserel(struct PlannerInfo *root,
						  struct RelOptInfo *rel)
{
	CdbPathLocus result;
	GpPolicy   *policy = rel->cdbpolicy;

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		CdbPathLocus_MakeEntry(&result);
		return result;
	}

	if (GpPolicyIsPartitioned(policy))
	{
		/* Are the rows distributed by hashing on specified columns? */
		if (policy->nattrs > 0)
		{
			List	   *distkeys = cdb_build_distribution_keys(root,
															   rel,
															   policy->nattrs,
															   policy->attrs);

			CdbPathLocus_MakeHashed(&result, distkeys, policy->numsegments);
		}

		/* Rows are distributed on an unknown criterion (uniformly, we hope!) */
		else
			CdbPathLocus_MakeStrewn(&result, policy->numsegments);
	}
	else if (GpPolicyIsReplicated(policy))
	{
		CdbPathLocus_MakeSegmentGeneral(&result, policy->numsegments);
	}
	/* Normal catalog access */
	else
		CdbPathLocus_MakeEntry(&result);

	return result;
}								/* cdbpathlocus_from_baserel */


/*
 * cdbpathlocus_from_exprs
 *
 * Returns a locus specifying hashed distribution on a list of exprs.
 */
CdbPathLocus
cdbpathlocus_from_exprs(struct PlannerInfo *root,
						List *hash_on_exprs,
						int numsegments)
{
	CdbPathLocus locus;
	List	   *distkeys = NIL;
	List	   *eq = list_make1(makeString("="));
	ListCell   *cell;

	foreach(cell, hash_on_exprs)
	{
		Node	   *expr = (Node *) lfirst(cell);
		DistributionKey *distkey;

		distkey = cdb_make_distkey_for_expr(root, expr, eq);
		distkeys = lappend(distkeys, distkey);
	}

	CdbPathLocus_MakeHashed(&locus, distkeys, numsegments);
	list_free_deep(eq);
	return locus;
}								/* cdbpathlocus_from_exprs */


/*
 * cdbpathlocus_from_subquery
 *
 * Returns a locus describing the distribution of a subquery.
 * The subquery plan should have been generated already.
 *
 * 'subqplan' is the subquery plan.
 * 'subqrelid' is the subquery RTE index in the current query level, for
 *      building Var nodes that reference the subquery's result columns.
 */
CdbPathLocus
cdbpathlocus_from_subquery(struct PlannerInfo *root,
						   struct Plan *subqplan,
						   Index subqrelid)
{
	CdbPathLocus locus;
	Flow	   *flow = subqplan->flow;
	int			numsegments;

	Insist(flow);

	/*
	 * We want to create a locus representing the subquery, so numsegments
	 * should be the same with the subquery.
	 */
	numsegments = flow->numsegments;

	/* Flow node was made from CdbPathLocus by cdbpathtoplan_create_flow() */
	switch (flow->flotype)
	{
		case FLOW_SINGLETON:
			if (flow->segindex == -1)
				CdbPathLocus_MakeEntry(&locus);
			else
			{
				/*
				 * keep segmentGeneral character, otherwise planner may put
				 * this subplan to qDisp unexpectedly 
				 */
				if (flow->locustype == CdbLocusType_SegmentGeneral)
					CdbPathLocus_MakeSegmentGeneral(&locus, numsegments);
				else
					CdbPathLocus_MakeSingleQE(&locus, numsegments);
			}
			break;
		case FLOW_REPLICATED:
			CdbPathLocus_MakeReplicated(&locus, numsegments);
			break;
		case FLOW_PARTITIONED:
			{
				List	   *distkeys = NIL;
				ListCell   *hashexprcell;
				List	   *eq = list_make1(makeString("="));

				foreach(hashexprcell, flow->hashExpr)
				{
					Node	   *expr = (Node *) lfirst(hashexprcell);
					TargetEntry *tle;
					Var		   *var;
					DistributionKey *distkey;

					/*
					 * Look for hash key expr among the subquery result
					 * columns.
					 */
					tle = tlist_member_ignore_relabel(expr, subqplan->targetlist);
					if (!tle)
						break;

					Assert(tle->resno >= 1);
					var = makeVar(subqrelid,
								  tle->resno,
								  exprType((Node *) tle->expr),
								  exprTypmod((Node *) tle->expr),
								  exprCollation((Node *) tle->expr),
								  0);
					distkey = cdb_make_distkey_for_expr(root, (Node *) var, eq);
					distkeys = lappend(distkeys, distkey);
				}
				if (distkeys &&
					!hashexprcell)
					CdbPathLocus_MakeHashed(&locus, distkeys, numsegments);
				else
					CdbPathLocus_MakeStrewn(&locus, numsegments);
				list_free_deep(eq);
				break;
			}
		default:
			CdbPathLocus_MakeNull(&locus, __GP_POLICY_EVIL_NUMSEGMENTS);
			Insist(0);
	}
	return locus;
}								/* cdbpathlocus_from_subquery */


/*
 * cdbpathlocus_get_distkey_exprs
 *
 * Returns a List with one Expr for each distkey column.  Each item either is
 * in the given targetlist, or has no Var nodes that are not in the targetlist;
 * and uses only rels in the given set of relids.  Returns NIL if the
 * distkey cannot be expressed in terms of the given relids and targetlist.
 */
List *
cdbpathlocus_get_distkey_exprs(CdbPathLocus locus,
							   Bitmapset *relids,
							   List *targetlist)
{
	List	   *result = NIL;
	ListCell   *distkeycell;

	Assert(cdbpathlocus_is_valid(locus));

	if (CdbPathLocus_IsHashed(locus))
	{
		foreach(distkeycell, locus.distkeys)
		{
			DistributionKey *distkey = (DistributionKey *) lfirst(distkeycell);
			Expr	   *item;

			item = cdbpullup_findEclassInTargetList(distkey->dk_eclass, targetlist);

			/*
			 * Fail if can't evaluate distribution key in the context of this
			 * targetlist.
			 */
			if (!item)
				return NIL;

			result = lappend(result, item);
		}
		return result;
	}
	else
		return NIL;
}								/* cdbpathlocus_get_distkey_exprs */


/*
 * cdbpathlocus_pull_above_projection
 *
 * Given a targetlist, and a locus evaluable before the projection is
 * applied, returns an equivalent locus evaluable after the projection.
 * Returns a strewn locus if the necessary inputs are not projected.
 *
 * 'relids' is the set of relids that may occur in the targetlist exprs.
 * 'targetlist' specifies the projection.  It is a List of TargetEntry
 *      or merely a List of Expr.
 * 'newvarlist' is an optional List of Expr, in 1-1 correspondence with
 *      'targetlist'.  If specified, instead of creating a Var node to
 *      reference a targetlist item, we plug in a copy of the corresponding
 *      newvarlist item.
 * 'newrelid' is the RTE index of the projected result, for finding or
 *      building Var nodes that reference the projected columns.
 *      Ignored if 'newvarlist' is specified.
 */
CdbPathLocus
cdbpathlocus_pull_above_projection(struct PlannerInfo *root,
								   CdbPathLocus locus,
								   Bitmapset *relids,
								   List *targetlist,
								   List *newvarlist,
								   Index newrelid)
{
	CdbPathLocus newlocus;
	ListCell   *distkeycell;
	List	   *newdistkeys = NIL;
	int			numsegments;

	Assert(cdbpathlocus_is_valid(locus));

	/*
	 * Keep the numsegments unchanged.
	 */
	numsegments = CdbPathLocus_NumSegments(locus);

	if (CdbPathLocus_IsHashed(locus))
	{
		foreach(distkeycell, locus.distkeys)
		{
			DistributionKey *olddistkey;
			DistributionKey *newdistkey;

			/* Get DistributionKey for key expr rewritten in terms of projection cols. */
			olddistkey = (DistributionKey *) lfirst(distkeycell);
			newdistkey = cdb_pull_up_distribution_key(root,
													  olddistkey,
													  relids,
													  targetlist,
													  newvarlist,
													  newrelid);

			/*
			 * Fail if can't evaluate distkey in the context of this
			 * targetlist.
			 */
			if (!newdistkey)
			{
				CdbPathLocus_MakeStrewn(&newlocus, numsegments);
				return newlocus;
			}

			/* Assemble new distkey. */
			newdistkeys = lappend(newdistkeys, newdistkey);
		}

		/* Build new locus. */
		CdbPathLocus_MakeHashed(&newlocus, newdistkeys, numsegments);
		return newlocus;
	}
	else
		return locus;
}								/* cdbpathlocus_pull_above_projection */


/*
 * cdbpathlocus_join
 *
 * Determine locus to describe the result of a join.  Any necessary Motion has
 * already been applied to the sources.
 */
CdbPathLocus
cdbpathlocus_join(RelOptInfo *joinrel, JoinType jointype, CdbPathLocus a, CdbPathLocus b)
{
	CdbPathLocus resultlocus = {0};
	int			numsegments;
	List	   *distkeys;
	ListCell   *acell,
		*bcell;

	Assert(joinrel->reloptkind == RELOPT_JOINREL);
	Assert(cdbpathlocus_is_valid(a));
	Assert(cdbpathlocus_is_valid(b));

	numsegments = CdbPathLocus_CommonSegments(a, b);

	/*
	 * SingleQE may have different segment counts.
	 */
	if (CdbPathLocus_IsSingleQE(a) &&
		CdbPathLocus_IsSingleQE(b))
	{
		CdbPathLocus_MakeSingleQE(&resultlocus, numsegments);
		return resultlocus;
	}

	/*
	 * If both are Entry then do the job on the common segments.
	 */
	if (CdbPathLocus_IsEntry(a) &&
		CdbPathLocus_IsEntry(b))
	{
		a.numsegments = numsegments;
		return a;
	}

	/*
	 * If one rel is general or replicated, result stays with the other rel,
	 * but need to ensure the result is on the common segments.
	 */
	if (CdbPathLocus_IsGeneral(a) ||
		CdbPathLocus_IsReplicated(a))
	{
		b.numsegments = numsegments;
		return b;
	}
	if (CdbPathLocus_IsGeneral(b) ||
		CdbPathLocus_IsReplicated(b))
	{
		a.numsegments = numsegments;
		return a;
	}

	/*
	 * FIXME: should we adjust the returned numsegments like
	 * Replicated above?
	 */
	if (CdbPathLocus_IsSegmentGeneral(a))
		return b;
	else if (CdbPathLocus_IsSegmentGeneral(b))
		return a;

	/*
	 * Both sides must be hashed, then. And the distribution keys should be
	 * compatible; otherwise the caller should not be building a join directly
	 * between the two rels. A Motion would be needed.
	 */
	Assert(CdbPathLocus_IsHashed(a));
	Assert(CdbPathLocus_IsHashed(b));
	Assert(CdbPathLocus_Degree(a) > 0 &&
		   CdbPathLocus_NumSegments(a) == CdbPathLocus_NumSegments(b) &&
		   CdbPathLocus_Degree(a) == CdbPathLocus_Degree(b));

	switch (jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
		case JOIN_DEDUP_SEMI:
		case JOIN_DEDUP_SEMI_REVERSE:
		case JOIN_UNIQUE_OUTER:
		case JOIN_UNIQUE_INNER:
			/*
			 * There's an equijoin predicate on the distribution keys, so the
			 * distribution key columns have the same values on both sides of
			 * the join. We could legitimately use the locus of either side to
			 * represent the distribution of the join rel. Decide which one to
			 * use.
			 *
			 * If both input rels have the exact same locus, there isn't really
			 * a choice to be made. This is the common case, with normal INNER
			 * JOINs, as the join predicate, e.g "a.col = b.col" between the two
			 * sides forms an equivalence class, and that equivalence class
			 * represents the distribution key of both sides.
			 */
			if (cdbpathlocus_equal(a, b))
				return a;

			/*
			 * In some cases, however, we might have an equijoin predicate between two
			 * different equivalence classes. That appens if one side of the join
			 * predicate is "outerjoin delayed". Usually, outerjoin delayed predicates
			 * occur with outer joins, by we can get here at least in some cases
			 * involving EXISTS and IN clauses that have been converted to joins.
			 *
			 * Try to choose the side that's more useful above the join, for matching
			 * with other joins above this one, or for grouping/sorting. We do that
			 * by looking at the EquivalenceClasses on both sides. If an EC refers to
			 * relations outside this join rel, then that seems useful for joining with
			 * those other relations, and we pick that EC.
			 */
			distkeys = NIL;
			forboth(acell, a.distkeys, bcell, b.distkeys)
			{
				DistributionKey *adistkey = (DistributionKey *) lfirst(acell);
				DistributionKey *bdistkey = (DistributionKey *) lfirst(bcell);
				DistributionKey *newdistkey;
				bool		a_is_useful_for_more_joins;
				bool		b_is_useful_for_more_joins;

				newdistkey = makeNode(DistributionKey);

				a_is_useful_for_more_joins = !bms_is_subset(adistkey->dk_eclass->ec_relids, joinrel->relids);
				b_is_useful_for_more_joins = !bms_is_subset(bdistkey->dk_eclass->ec_relids, joinrel->relids);

				if (a_is_useful_for_more_joins && b_is_useful_for_more_joins)
				{
					/*
					 * This shouldn't happen, the inner side of a left outer
					 * join should not have any EC members from the outer side.
					 * Or maybe there are cases, but I haven't been able to
					 * construct one. But if it does happen, let's not panic.
					 * Remember, we can legitimately use either EC to represent
					 * this, so arbitrarily pick one of them. This might not be
					 * optimal, but it's correct.
					 *
					 * In testing, it would be nice to find out if this ever
					 * happens, though, hence the Assert.
					 */
					Assert(false);
					elog(DEBUG1, "both ECs representing the distribution key of a join would be useful for more joins");
					newdistkey->dk_eclass = adistkey->dk_eclass;
				}
				else if (a_is_useful_for_more_joins)
					newdistkey->dk_eclass = adistkey->dk_eclass;
				else if (b_is_useful_for_more_joins)
					newdistkey->dk_eclass = bdistkey->dk_eclass;
				else
				{
					/* Neither side seems useful for further joins. Arbitrarily, pick 'a' */
					/* FIXME: Might one side be useful for sorting/grouping? */
					newdistkey->dk_eclass = adistkey->dk_eclass;
				}

				distkeys = lappend(distkeys, newdistkey);
			}
			CdbPathLocus_MakeHashed(&resultlocus, distkeys, numsegments);
			break;

		case JOIN_LEFT:
		case JOIN_LASJ_NOTIN:
		case JOIN_ANTI:
			/* Use the locus of the outer side. */
			resultlocus = a;
			break;

		case JOIN_RIGHT:
			/* Use the locus of the outer side */
			resultlocus = b;
			break;

		case JOIN_FULL:
			/*
			 * In a FULL JOIN, the non-null rows are distributed according the
			 * distribution key of either side. But NULL rows can appear in
			 * any segment, i.e. they are not properly distributed according
			 * to any distribution key. We have to therefore mark the locus as
			 * strewn.
			 */
			CdbPathLocus_MakeStrewn(&resultlocus, numsegments);
			break;
	}

	Assert(cdbpathlocus_is_valid(resultlocus));
	return resultlocus;
}								/* cdbpathlocus_join */

/*
 * cdbpathlocus_is_hashed_on_exprs
 *
 * This function tests whether grouping on a given set of exprs can be done
 * in place without motion.
 *
 * For a hashed locus, returns false if the distkey has a column whose
 * equivalence class contains no expr belonging to the given list.
 *
 * If 'ignore_constants' is true, any constants in the locus are ignored.
 */
bool
cdbpathlocus_is_hashed_on_exprs(CdbPathLocus locus, List *exprlist,
								   bool ignore_constants)
{
	ListCell   *distkeycell;

	Assert(cdbpathlocus_is_valid(locus));

	if (CdbPathLocus_IsHashed(locus))
	{
		foreach(distkeycell, locus.distkeys)
		{
			DistributionKey *distkey = (DistributionKey *) lfirst(distkeycell);
			bool		found = false;
			ListCell   *i;

			/* Does distribution key have an expr that is equal() to one in exprlist? */

			Assert(IsA(distkey, DistributionKey));

			if (ignore_constants && CdbEquivClassIsConstant(distkey->dk_eclass))
				continue;

			foreach(i, distkey->dk_eclass->ec_members)
			{
				EquivalenceMember *em = (EquivalenceMember *) lfirst(i);

				if (list_member(exprlist, em->em_expr))
				{
					found = true;
					break;
				}
			}
			if (!found)
				return false;
		}
		/* Every column of the distkey contains an expr in exprlist. */
		return true;
	}
	else
		return !CdbPathLocus_IsStrewn(locus);
}								/* cdbpathlocus_is_hashed_on_exprs */

/*
 * cdbpathlocus_is_hashed_on_eclasses
 *
 * This function tests whether grouping on a given set of exprs can be done
 * in place without motion.
 *
 * For a hashed locus, returns false if the distribution key has any column whose
 * equivalence class is not in 'eclasses' list.
 *
 * If 'ignore_constants' is true, any constants in the locus are ignored.
 */
bool
cdbpathlocus_is_hashed_on_eclasses(CdbPathLocus locus, List *eclasses,
								   bool ignore_constants)
{
	ListCell   *distkeycell;
	ListCell   *eccell;

	Assert(cdbpathlocus_is_valid(locus));

	if (CdbPathLocus_IsHashed(locus))
	{
		foreach(distkeycell, locus.distkeys)
		{
			DistributionKey *distkey = (DistributionKey *) lfirst(distkeycell);
			bool		found = false;
			EquivalenceClass *dk_eclass = distkey->dk_eclass;

			/* Does distkey have an eclass that's not in 'eclasses'? */
			Assert(IsA(distkey, DistributionKey));

			while (dk_eclass->ec_merged != NULL)
				dk_eclass = dk_eclass->ec_merged;

			if (ignore_constants && CdbEquivClassIsConstant(dk_eclass))
				continue;

			foreach(eccell, eclasses)
			{
				EquivalenceClass *ec = (EquivalenceClass *) lfirst(eccell);

				while (ec->ec_merged != NULL)
					ec = ec->ec_merged;

				if (ec == dk_eclass)
				{
					found = true;
					break;
				}
			}
			if (!found)
				return false;
		}
		/* Every column of the distribution key contains an expr in exprlist. */
		return true;
	}
	else
		return !CdbPathLocus_IsStrewn(locus);
}								/* cdbpathlocus_is_hashed_on_exprs */


/*
 * cdbpathlocus_is_hashed_on_relids
 *
 * Used when a subquery predicate (such as "x IN (SELECT ...)") has been
 * flattened into a join with the tables of the current query.  A row of
 * the cross-product of the current query's tables might join with more
 * than one row from the subquery and thus be duplicated.  Removing such
 * duplicates after the join is called deduping.  If a row's duplicates
 * might occur in more than one partition, a Motion operator will be
 * needed to bring them together.  This function tests whether the join
 * result (whose locus is given) can be deduped in place without motion.
 *
 * For a hashed locus, returns false if the distkey has a column whose
 * equivalence class contains no Var node belonging to the given set of
 * relids.  Caller should specify the relids of the non-subquery tables.
 */
bool
cdbpathlocus_is_hashed_on_relids(CdbPathLocus locus, Bitmapset *relids)
{
	Assert(cdbpathlocus_is_valid(locus));

	if (CdbPathLocus_IsHashed(locus))
	{
		ListCell   *lc1;

		foreach(lc1, locus.distkeys)
		{
			/* Does distribution key contain a Var whose varno is in relids? */
			DistributionKey *distkey = (DistributionKey *) lfirst(lc1);
			bool		found = false;
			ListCell   *lc2;

			Assert(IsA(distkey, DistributionKey));

			foreach(lc2, distkey->dk_eclass->ec_members)
			{
				EquivalenceMember *em = (EquivalenceMember *) lfirst(lc2);

				if (IsA(em->em_expr, Var) &&bms_is_subset(em->em_relids, relids))
				{
					found = true;
					break;
				}
			}
			if (!found)
				return false;
		}

		/*
		 * Every column of the distkey contains a Var whose varno is in
		 * relids.
		 */
		return true;
	}
	else
		return !CdbPathLocus_IsStrewn(locus);
}								/* cdbpathlocus_is_hashed_on_relids */


/*
 * cdbpathlocus_is_valid
 *
 * Returns true if locus appears structurally valid.
 */
bool
cdbpathlocus_is_valid(CdbPathLocus locus)
{
	ListCell   *distkeycell;

	if (!CdbLocusType_IsValid(locus.locustype))
		goto bad;

	if (!CdbPathLocus_IsHashed(locus) && locus.distkeys != NIL)
		goto bad;

	if (CdbPathLocus_IsHashed(locus))
	{
		if (locus.distkeys == NIL)
			goto bad;
		if (!IsA(locus.distkeys, List))
			goto bad;
		foreach(distkeycell, locus.distkeys)
		{
			DistributionKey *item = (DistributionKey *) lfirst(distkeycell);

			if (!item || !IsA(item, DistributionKey))
				goto bad;
		}
	}
	return true;

bad:
	return false;
}								/* cdbpathlocus_is_valid */

List *
cdbpathlocus_get_distkeys_for_pathkeys(List *pathkeys)
{
	ListCell   *lc;
	List	   *result = NIL;

	foreach(lc, pathkeys)
	{
		PathKey *pk = (PathKey *) lfirst(lc);
		DistributionKey *dk;

		dk = makeNode(DistributionKey);
		dk->dk_eclass = pk->pk_eclass;
		result = lappend(result, dk);
	}

	return result;
}
