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
 * Are two distkeys equal?
 *
 * This is roughly the same as compare_pathkeys, but compare_pathkeys
 * requires the input pathkeys to be canonicalized. The PathKeys we use to
 * represent hashed distribution are not canonicalized, so we can't use it.
 *
 * FIXME: there's no concept of canonicalization for dist keys, is there?
 */
static bool
distkeys_equal(List *adistkeys, List *bdistkeys)
{
	ListCell   *acell;
	ListCell   *bcell;

	forboth(acell, adistkeys, bcell, bdistkeys)
	{
		DistributionKey *adistkey = (DistributionKey *) lfirst(acell);
		DistributionKey *bdistkey = (DistributionKey *) lfirst(bcell);

		Assert(IsA(adistkey, DistributionKey));
		Assert(IsA(bdistkey, DistributionKey));

		if (adistkey->dk_eclass != bdistkey->dk_eclass)
			return false;
	}
	return true;
}

/*
 * Does given list of distribution keys contain the given key? "contains" is
 * defined in terms of distkeys_equal.
 */
static bool
list_contains_distkey(List *list, List *distkey)
{
	ListCell   *lc;
	bool		found;

	found = false;
	foreach(lc, list)
	{
		List	   *ldistkey = (List *) lfirst(lc);

		if (distkeys_equal(distkey, ldistkey))
		{
			found = true;
			break;
		}
	}
	return found;
}

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
 *      is equal to the set produced by b's partkey.
 *
 *    - Returns false otherwise.
 */
bool
cdbpathlocus_equal(CdbPathLocus a, CdbPathLocus b)
{
	ListCell   *acell;
	ListCell   *bcell;
	ListCell   *aequivdistkeycell;
	ListCell   *bequivdistkeycell;

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
			return distkeys_equal(a.distkey_h, b.distkey_h);

		if (CdbPathLocus_IsHashedOJ(a))
		{
			forboth(acell, a.distkey_oj, bcell, b.distkey_oj)
			{
				List	   *aequivdistkeylist = (List *) lfirst(acell);
				List	   *bequivdistkeylist = (List *) lfirst(bcell);

				foreach(bequivdistkeycell, bequivdistkeylist)
				{
					List	   *bdistkey = (List *) lfirst(bequivdistkeycell);

					if (!list_contains_distkey(aequivdistkeylist, bdistkey))
						return false;
				}
				foreach(aequivdistkeycell, aequivdistkeylist)
				{
					List	   *adistkey = (List *) lfirst(aequivdistkeycell);

					if (!list_contains_distkey(bequivdistkeylist, adistkey))
						return false;
				}
			}
			return true;
		}
	}

	if (CdbPathLocus_IsHashedOJ(a) &&
		CdbPathLocus_IsHashed(b))
	{
		CdbSwap(CdbPathLocus, a, b);
	}

	if (CdbPathLocus_IsHashed(a) &&
		CdbPathLocus_IsHashedOJ(b))
	{
		forboth(acell, a.distkey_h, bcell, b.distkey_oj)
		{
			List	   *adistkey = (List *) lfirst(acell);
			List	   *bequivdistkeylist = (List *) lfirst(bcell);

			foreach(bequivdistkeycell, bequivdistkeylist)
			{
				List	   *bdistkey = (List *) lfirst(bequivdistkeycell);

				if (adistkey != bdistkey)
					return false;
			}
		}
		return true;
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
		foreach(distkeycell, locus.distkey_h)
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
	else if (CdbPathLocus_IsHashedOJ(locus))
	{
		foreach(distkeycell, locus.distkey_oj)
		{
			List	   *distkeylist = (List *) lfirst(distkeycell);
			ListCell   *distkeylistcell;
			Expr	   *item = NULL;

			foreach(distkeylistcell, distkeylist)
			{
				DistributionKey *distkey = (DistributionKey *) lfirst(distkeylistcell);

				item = cdbpullup_findEclassInTargetList(distkey->dk_eclass, targetlist);

				if (item)
				{
					result = lappend(result, item);
					break;
				}
			}
			if (!item)
				return NIL;
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
		foreach(distkeycell, locus.distkey_h)
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
	else if (CdbPathLocus_IsHashedOJ(locus))
	{
		/* For each column of the distribution key... */
		foreach(distkeycell, locus.distkey_oj)
		{
			DistributionKey *olddistkey;
			DistributionKey *newdistkey = NULL;

			/* Get DistributionKey for key expr rewritten in terms of projection cols. */
			List	   *distkeylist = (List *) lfirst(distkeycell);
			ListCell   *distkeylistcell;

			foreach(distkeylistcell, distkeylist)
			{
				olddistkey = (DistributionKey *) lfirst(distkeylistcell);
				newdistkey = cdb_pull_up_distribution_key(root,
														  olddistkey,
														  relids,
														  targetlist,
														  newvarlist,
														  newrelid);
				if (newdistkey)
					break;
			}

			/*
			 * NB: Targetlist might include columns from both sides of outer
			 * join "=" comparison, in which case cdb_pull_up_distribution_keys might
			 * succeed on keys from more than one distkeylist. The
			 * pulled-up locus could then be a HashedOJ locus, perhaps saving
			 * a Motion when an outer join is followed by UNION ALL followed
			 * by a join or aggregate.  For now, don't bother.
			 */

			/*
			 * Fail if can't evaluate distribution key in the context of this
			 * targetlist.
			 */
			if (!newdistkey)
			{
				CdbPathLocus_MakeStrewn(&newlocus, numsegments);
				return newlocus;
			}

			/* Assemble new distkeys. */
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
cdbpathlocus_join(CdbPathLocus a, CdbPathLocus b)
{
	ListCell   *acell;
	ListCell   *bcell;
	List	   *equivdistkeylist;
	CdbPathLocus ojlocus = {0};
	int			numsegments;

	Assert(cdbpathlocus_is_valid(a));
	Assert(cdbpathlocus_is_valid(b));

	/* Do both input rels have same locus? */
	if (cdbpathlocus_equal(a, b))
		return a;

	numsegments = CdbPathLocus_CommonSegments(a, b);

	/*
	 * SingleQE may have different segment counts.
	 */
	if (CdbPathLocus_IsSingleQE(a) &&
		CdbPathLocus_IsSingleQE(b))
	{
		CdbPathLocus_MakeSingleQE(&ojlocus, numsegments);
		return ojlocus;
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
	 * This is an outer join, or one or both inputs are outer join results.
	 * And a and b are on the same segments.
	 */

	Assert(CdbPathLocus_Degree(a) > 0 &&
		   CdbPathLocus_NumSegments(a) == CdbPathLocus_NumSegments(b) &&
		   CdbPathLocus_Degree(a) == CdbPathLocus_Degree(b));

	if (CdbPathLocus_IsHashed(a) &&
		CdbPathLocus_IsHashed(b))
	{
		/* Zip the two distkey lists together to make a HashedOJ locus. */
		List	   *distkey_oj = NIL;

		forboth(acell, a.distkey_h, bcell, b.distkey_h)
		{
			DistributionKey *adistkey = (DistributionKey *) lfirst(acell);
			DistributionKey *bdistkey = (DistributionKey *) lfirst(bcell);

			equivdistkeylist = list_make2(adistkey, bdistkey);
			distkey_oj = lappend(distkey_oj, equivdistkeylist);
		}
		CdbPathLocus_MakeHashedOJ(&ojlocus, distkey_oj, numsegments);
		Assert(cdbpathlocus_is_valid(ojlocus));
		return ojlocus;
	}

	if (!CdbPathLocus_IsHashedOJ(a))
		CdbSwap(CdbPathLocus, a, b);

	Assert(CdbPathLocus_IsHashedOJ(a));
	Assert(CdbPathLocus_IsHashed(b) ||
		   CdbPathLocus_IsHashedOJ(b));

	if (CdbPathLocus_IsHashed(b))
	{
		List	   *distkey_oj = NIL;

		forboth(acell, a.distkey_oj, bcell, b.distkey_h)
		{
			List	   *aequivdistkeylist = (List *) lfirst(acell);
			DistributionKey *bdistkey = (DistributionKey *) lfirst(bcell);

			equivdistkeylist = lappend(list_copy(aequivdistkeylist), bdistkey);
			distkey_oj = lappend(distkey_oj, equivdistkeylist);
		}
		CdbPathLocus_MakeHashedOJ(&ojlocus, distkey_oj, numsegments);
	}
	else if (CdbPathLocus_IsHashedOJ(b))
	{
		List	   *distkey_oj = NIL;

		forboth(acell, a.distkey_oj, bcell, b.distkey_oj)
		{
			List	   *aequivdistkeylist = (List *) lfirst(acell);
			List	   *bequivdistkeylist = (List *) lfirst(bcell);

			equivdistkeylist = list_union_ptr(aequivdistkeylist,
											  bequivdistkeylist);
			distkey_oj = lappend(distkey_oj, equivdistkeylist);
		}
		CdbPathLocus_MakeHashedOJ(&ojlocus, distkey_oj, numsegments);
	}
	Assert(cdbpathlocus_is_valid(ojlocus));
	return ojlocus;
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
		foreach(distkeycell, locus.distkey_h)
		{
			bool		found = false;
			ListCell   *i;

			/* Does distribution key have an expr that is equal() to one in exprlist? */
			DistributionKey *distkey = (DistributionKey *) lfirst(distkeycell);

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
	else if (CdbPathLocus_IsHashedOJ(locus))
	{
		foreach(distkeycell, locus.distkey_oj)
		{
			List	   *distkeylist = (List *) lfirst(distkeycell);
			ListCell   *distkeylistcell;
			bool		found = false;

			foreach(distkeylistcell, distkeylist)
			{
				/* Does some expr in distkey match some item in exprlist? */
				DistributionKey *item = (DistributionKey *) lfirst(distkeylistcell);
				ListCell   *i;

				Assert(IsA(item, DistributionKey));

				if (ignore_constants && CdbEquivClassIsConstant(item->dk_eclass))
					continue;

				foreach(i, item->dk_eclass->ec_members)
				{
					EquivalenceMember *em = (EquivalenceMember *) lfirst(i);

					if (list_member(exprlist, em->em_expr))
					{
						found = true;
						break;
					}
				}
				if (found)
					break;
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
		foreach(distkeycell, locus.distkey_h)
		{
			DistributionKey *distkey = (DistributionKey *) lfirst(distkeycell);
			bool		found = false;
			EquivalenceClass *dk_ec;

			/* Does distkey have an eclass that's not in 'eclasses'? */
			Assert(IsA(distkey, DistributionKey));

			dk_ec = distkey->dk_eclass;
			while (dk_ec->ec_merged != NULL)
				dk_ec = dk_ec->ec_merged;

			if (ignore_constants && CdbEquivClassIsConstant(dk_ec))
				continue;

			foreach(eccell, eclasses)
			{
				EquivalenceClass *ec = (EquivalenceClass *) lfirst(eccell);

				while (ec->ec_merged != NULL)
					ec = ec->ec_merged;

				if (ec == dk_ec)
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
	else if (CdbPathLocus_IsHashedOJ(locus))
	{
		foreach(distkeycell, locus.distkey_oj)
		{
			List	   *distkeylist = (List *) lfirst(distkeycell);
			ListCell   *distkeylistcell;
			bool		found = false;

			foreach(distkeylistcell, distkeylist)
			{
				DistributionKey *distkey = (DistributionKey *) lfirst(distkeylistcell);
				EquivalenceClass *dk_ec;

				/* Does distkey have an eclass that's not in 'eclasses'? */
				Assert(IsA(distkey, DistributionKey));

				dk_ec = distkey->dk_eclass;
				while (dk_ec->ec_merged != NULL)
					dk_ec = dk_ec->ec_merged;

				if (ignore_constants && CdbEquivClassIsConstant(dk_ec))
					continue;

				foreach(eccell, eclasses)
				{
					EquivalenceClass *ec = (EquivalenceClass *) lfirst(eccell);

					while (ec->ec_merged != NULL)
						ec = ec->ec_merged;

					if (ec == dk_ec)
					{
						found = true;
						break;
					}
				}
				if (found)
					break;
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

		foreach(lc1, locus.distkey_h)
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
	else if (CdbPathLocus_IsHashedOJ(locus))
	{
		ListCell   *lc1;

		foreach(lc1, locus.distkey_oj)
		{
			bool		found = false;
			List	   *distkeylist = (List *) lfirst(lc1);
			ListCell   *lc2;

			foreach(lc2, distkeylist)
			{
				/* Does distribution key contain a Var whose varno is in relids? */
				DistributionKey *item = (DistributionKey *) lfirst(lc2);
				ListCell   *lc3;

				Assert(IsA(item, DistributionKey));

				foreach(lc3, item->dk_eclass->ec_members)
				{
					EquivalenceMember *em = (EquivalenceMember *) lfirst(lc3);

					if (IsA(em->em_expr, Var) &&bms_is_subset(em->em_relids, relids))
					{
						found = true;
						break;
					}
				}
				if (found)
					break;
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

	if (!CdbPathLocus_IsHashed(locus) && locus.distkey_h != NIL)
		goto bad;
	if (!CdbPathLocus_IsHashedOJ(locus) && locus.distkey_oj != NIL)
		goto bad;

	if (CdbPathLocus_IsHashed(locus))
	{
		if (locus.distkey_h == NIL)
			goto bad;
		if (!IsA(locus.distkey_h, List))
			goto bad;
		foreach(distkeycell, locus.distkey_h)
		{
			DistributionKey *item = (DistributionKey *) lfirst(distkeycell);

			if (!item || !IsA(item, DistributionKey))
				goto bad;
		}
	}
	if (CdbPathLocus_IsHashedOJ(locus))
	{
		if (locus.distkey_oj == NIL)
			goto bad;
		if (!IsA(locus.distkey_oj, List))
			goto bad;
		foreach(distkeycell, locus.distkey_oj)
		{
			List	   *item = (List *) lfirst(distkeycell);

			if (!item || !IsA(item, List))
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
