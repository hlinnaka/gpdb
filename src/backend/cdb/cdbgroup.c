/*-------------------------------------------------------------------------
 *
 * cdbgroup.c
 *	  Helper functions for dealing with grouping.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbgroup.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/planpartition.h"
#include "optimizer/planshare.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "catalog/pg_aggregate.h"

#include "cdb/cdbllize.h"
#include "cdb/cdbpathtoplan.h"  /* cdbpathtoplan_create_flow() */
#include "cdb/cdbpath.h"
#include "cdb/cdbplan.h"
#include "cdb/cdbpullup.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbhash.h"        /* isGreenplumDbHashable() */

#include "cdb/cdbsetop.h"
#include "cdb/cdbgroup.h"

typedef struct
{
	PlannerInfo *root;
	List	   *partial_tlist;

	bool		found_aggrefs;
} make_partial_agg_targetlist_context;

static Plan *
make_appropriate_agg(PlannerInfo *root,
					 AggStage aggstage,
					 bool use_hashed_grouping,
					 bool need_sort_for_grouping,
					 List *tlist,
					 List *havingQual,
					 int numGroupCols,
					 AttrNumber *grpColIdx,
					 Oid *grpOperators,
					 long numGroups,
					 int numAggs,
					 Plan *result_plan,
					 List *group_pathkeys,
					 List **current_pathkeys, CdbPathLocus *current_locus);

static Plan *
ensure_collocation(PlannerInfo *root,
				   Plan *result_plan,
				   List *tlist,
				   List *group_pathkeys,
				   List **current_pathkeys,
				   CdbPathLocus *current_locus);
static void
make_two_stage_aggref_targetlists(PlannerInfo *root,
								  List *orig_tlist,
								  List *orig_havingQual,
								  WindowFuncLists *wflists,
								  List **partial_tlist,
								  List **final_tlist,
								  List **final_havingQual);
static Node *
replace_stage_aggrefs_mutator(Node *node, make_partial_agg_targetlist_context *context);

static Oid
lookup_agg_transtype(Aggref *aggref);

typedef struct
{
	PlannerInfo *root;

	bool		has_aggs;				/* any aggregates at all? */
	bool		has_ordered_aggs;		/* aggregates with ORDER BY args? */
	bool		has_distinct_aggs;		/* aggregates with DISTINCT args? */
	bool		has_missing_prelim_aggs;	/* aggregates without prelimfn? */

	/* pathkeys representing a grouping required for ordered-set aggregates. */
	bool		mismatched_distinct_pathkeys;
	List	   *agg_distinct_pathkeys;
} analyze_aggs_context;

static bool
analyze_aggs_walker(Node *node, analyze_aggs_context *cxt)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		Aggref	   *aggref = (Aggref *) node;

		if (aggref->agglevelsup == 0)
		{
			HeapTuple	tuple;
			Form_pg_aggregate aggform;

			cxt->has_aggs = true;

			if (aggref->aggorder)
				cxt->has_ordered_aggs = true;
			if (aggref->aggdistinct)
				cxt->has_distinct_aggs = true;

			if (aggref->aggdistinct && !cxt->mismatched_distinct_pathkeys)
			{
				List	   *pk;
				PathKeysComparison cmp;

				pk = make_pathkeys_for_sortclauses(cxt->root,
												   aggref->aggdistinct,
												   aggref->args,
												   true);
				if (!cxt->agg_distinct_pathkeys)
					cxt->agg_distinct_pathkeys = pk;
				else
				{
					cmp = compare_pathkeys(pk, cxt->agg_distinct_pathkeys);
					if (cmp == PATHKEYS_DIFFERENT)
					{
						cxt->mismatched_distinct_pathkeys = true;
						cxt->agg_distinct_pathkeys = NIL;
					}
					else if (cmp == PATHKEYS_BETTER2)
					{
						/* this pathkey is a prefix of the old one */
						cxt->agg_distinct_pathkeys = pk;
					}
					else
					{
						Assert(cmp == PATHKEYS_BETTER1 || cmp == PATHKEYS_EQUAL);
					}
				}
			}

			tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid));
			if (!tuple)
				elog(ERROR, "cache lookup failed for aggregate %u", aggref->aggfnoid);
			aggform = ((Form_pg_aggregate) GETSTRUCT(tuple));

			if (aggform->aggprelimfn == InvalidOid)
				cxt->has_missing_prelim_aggs = true;

			ReleaseSysCache(tuple);
		}
	}
	return expression_tree_walker(node, analyze_aggs_walker, cxt);
}

/*
 *
 *
 */
Plan *
cdb_grouping_planner(PlannerInfo *root,
					 List *tlist,
					 Plan *result_plan,
					 List *group_pathkeys,
					 WindowFuncLists *wflists,
					 List **current_pathkeys,
					 CdbPathLocus *current_locus,
					 int numGroupCols,
					 AttrNumber *groupColIdx,
					 Oid *groupOperators,
					 bool need_sort_for_grouping,
					 bool use_hashed_grouping,
					 long numGroups /* estimate only */
	)
{
	Query	   *parse = root->parse;
	analyze_aggs_context analysis;

	Assert(parse->hasAggs || parse->groupClause);

	/*
	 * 1. Search the target list for any Aggrefs.
	 *
	 * In the top-level, FINAL aggregate, (or INTERMEDIATE) any Aggrefs in the targetlist
	 * actually refer to pre-aggregated result of the Agg node somewhere below.
	 * set_plan_references() will fix up the target list of the aggregate, replacing the
	 * arguments of Aggrefs with Vars that point to the result of the level below.
	 *
	 * In the PARTIAL stage (or NORMAL), the aggregates are evaluated normally.
	 *
	 * Do *all* the aggregate functions have preliminary support? If not, we have to
	 * evaluate it normally, as one-stage.
	 */

	memset(&analysis, 0, sizeof(analyze_aggs_context));
	analysis.root = root;

	/*
	 * Scan all the parts of the query. This should cover all the same places that
	 * make_two_stage_aggref_targetlists mutates.
	 */
	analyze_aggs_walker((Node *) tlist, &analysis);
	analyze_aggs_walker((Node *) parse->havingQual, &analysis);
	if (wflists)
	{
		int			i;

		for (i = 0; i < wflists->maxWinRef; i++)
			analyze_aggs_walker((Node *) wflists->windowFuncs[i], &analysis);
	}

	/*
	 * Create one of the following plans:
	 *
	 * One-stage:
	 *
	 * For this, we just ensure that the input is distributed correctly (matching
	 * the GROUP BY), and return NULL. The normal Postgres code in grouping_planner()
	 * will do the rest.
	 *
	 * Two-stage:
	 *
	 *
	 *
	 *
	 * Three-stage:
	 *
	 *  Final Agg <- Partial Agg <- Motion <- Distinct
	 */

	if (Gp_role != GP_ROLE_DISPATCH)
		goto one_stage;

	if (!root->config->gp_enable_multiphase_agg)
		goto one_stage;

	/*
	 * If an aggregate doesn't have a preliminary function, we can't split
	 * it.
	 */
	if (analysis.has_missing_prelim_aggs)
		goto one_stage;

	/*
	 * Ordered aggregates need to run the transition function on the values
	 * in sorted order, which in turn translates into single phase
	 * aggregation.
	 */
	if (analysis.has_ordered_aggs)
		goto one_stage;

	/*
	 * If all the data is focused on a single node, just run the Agg there.
	 * In theory it might be better to redistribute it, so that the Agg could
	 * run in parallel, but an extra Motion is quite expensive so it is
	 * unlikely to be win.
	 */
	if (!CdbPathLocus_IsPartitioned(*current_locus))
		goto one_stage;


	/*
	 * If the input's distribution matches the GROUP BY, then we can
	 * run the aggregate separately on each node, and just append the
	 * results.
	 */
	if (group_pathkeys &&
		cdbpathlocus_collocates(root, *current_locus, group_pathkeys, false))
		goto one_stage;

	/*
	 * If there's a DISTINCT agg, but no GROUP BY, then we can perform a
	 * two-stage agg, if the data distribution matches the DISTINCT.
	 */
	if (analysis.has_distinct_aggs)
	{
		if (group_pathkeys)
		{
			/*
			 * If there is a DISTINCT agg and a GROUP BY, we can do a two-phase
			 * agg if the data is already distributed by the GROUP BY key.
			 */
			if (!cdbpathlocus_collocates(root, *current_locus, group_pathkeys, false))
				goto one_stage;
		}
		else
		{
			/*
			 * No GROUP BY. We can still do a two-phase agg, if the data is already
			 * distributed on the DISTINCT key.
			 */
			if (analysis.mismatched_distinct_pathkeys ||
				!cdbpathlocus_collocates(root, *current_locus,
										 analysis.agg_distinct_pathkeys, false))
				goto one_stage;
		}
	}

	/* all clear to do a two stage agg */
	goto two_stage;

	/**** One-stage agg ****/
one_stage:
	{
		result_plan = ensure_collocation(root,
										 result_plan,
										 tlist,
										 group_pathkeys,
										 current_pathkeys,
										 current_locus);

		result_plan = make_appropriate_agg(root,
										   AGGSTAGE_NORMAL,
										   use_hashed_grouping,
										   need_sort_for_grouping,
										   tlist,
										   (List *) parse->havingQual,
										   numGroupCols,
										   groupColIdx,
										   groupOperators,
										   numGroups,
										   1, /* FIXME: agg_counts.numAggs */
										   result_plan,
										   group_pathkeys,
										   current_pathkeys,
										   current_locus);
		return result_plan;
	}

	/**** Two-stage agg ****/
two_stage:
	{
		List	   *partial_tlist;
		List	   *final_tlist;
		List	   *final_havingQual;
		AttrNumber *finalGroupColIdx;

		//elog(NOTICE, "resulttlist  : %s", nodeToString(result_plan->targetlist));

		/* 0. Create the target lists */
		make_two_stage_aggref_targetlists(root,
										  tlist,
										  (List *) parse->havingQual,
										  wflists,
										  &partial_tlist,
										  &final_tlist,
										  &final_havingQual);

		/* 1. Create the preliminary Agg node. */
		// TODO hashed grouping.

		result_plan = make_appropriate_agg(root,
										   AGGSTAGE_PARTIAL,
										   use_hashed_grouping,
										   need_sort_for_grouping,
										   partial_tlist,
										   NIL, /* havingQual is handled in the final stage */
										   numGroupCols,
										   groupColIdx,
										   groupOperators,
										   numGroups,
										   1, /* FIXME: agg_counts.numAggs */
										   result_plan,
										   group_pathkeys,
										   current_pathkeys,
										   current_locus);

		/* 2. Redistribute. */
		result_plan = ensure_collocation(root,
										 result_plan,
										 tlist,
										 group_pathkeys,
										 current_pathkeys,
										 current_locus);

		/* 3. Final Agg */

		/*
		 * The input to the final Agg is the output of the preliminary Agg+Motion,
		 * so the grouping columns are in different order than in the preliminary Agg.
		 */
		if (numGroupCols > 0)
		{
			finalGroupColIdx = (AttrNumber *) palloc(numGroupCols * sizeof(AttrNumber));
			locate_grouping_columns(root, tlist, partial_tlist, finalGroupColIdx);
		}
		else
			finalGroupColIdx = NULL;

		result_plan = make_appropriate_agg(root,
										   AGGSTAGE_FINAL,
										   use_hashed_grouping,
										   need_sort_for_grouping,
										   final_tlist,
										   final_havingQual,
										   numGroupCols,
										   finalGroupColIdx,
										   groupOperators,
										   numGroups,
										   1, /* FIXME: agg_counts.numAggs */
										   result_plan,
										   group_pathkeys,
										   current_pathkeys,
										   current_locus);
		return result_plan;
	}
}

static Plan *
make_appropriate_agg(PlannerInfo *root,
					 AggStage aggstage,
					 bool use_hashed_grouping,
					 bool need_sort_for_grouping,
					 List *tlist,
					 List *havingQual,
					 int numGroupCols,
					 AttrNumber *grpColIdx,
					 Oid *grpOperators,
					 long numGroups,
					 int numAggs,
					 Plan *result_plan,
					 List *group_pathkeys,
					 List **current_pathkeys, CdbPathLocus *current_locus)
{
	Query	   *parse = root->parse;
	Agg		   *result_agg;

	/*
	 * Insert AGG or GROUP node if needed, plus an explicit sort step
	 * if necessary.
	 *
	 * HAVING clause, if any, becomes qual of the Agg or Group node.
	 */
	if (use_hashed_grouping)
	{
		/* Hashed aggregate plan --- no sort needed */
		result_agg = make_agg(root,
							  tlist,
							  havingQual,
							  AGG_HASHED, false,
							  numGroupCols,
							  grpColIdx,
							  grpOperators,
							  numGroups,
							  numAggs,
							  result_plan);
		result_agg->aggstage = aggstage;
		result_plan = (Plan *) result_agg;

		/* Hashed aggregation produces randomly-ordered results */
		*current_pathkeys = NIL;
	}
	else if (parse->hasAggs || parse->groupClause)
	{
		/* Plain aggregate plan --- sort if needed */
		AggStrategy aggstrategy;

		if (parse->groupClause)
		{
			if (need_sort_for_grouping)
			{
				result_plan = (Plan *)
					make_sort_from_groupcols(root,
											 parse->groupClause,
											 grpColIdx,
											 result_plan);
				*current_pathkeys = group_pathkeys;

				/* Decorate the Sort node with a Flow node. */
				mark_sort_locus(result_plan);
			}
			aggstrategy = AGG_SORTED;

			/*
			 * The AGG node will not change the sort ordering of its
			 * groups, so current_pathkeys describes the result too.
			 */
		}
		else
		{
			aggstrategy = AGG_PLAIN;
			/* Result will be only one row anyway; no sort order */
			*current_pathkeys = NIL;
		}

		result_agg = make_agg(root,
							  tlist,
							  havingQual,
							  aggstrategy, false,
							  numGroupCols,
							  grpColIdx,
							  grpOperators,
							  numGroups,
							  numAggs,
							  result_plan);
		result_agg->aggstage = aggstage;
		result_plan = (Plan *) result_agg;
	}
	else
		elog(ERROR, "cdb_grouping_planner called but no grouping is required");

	result_plan->flow = pull_up_Flow(result_plan, result_plan->lefttree);

	return result_plan;
}

static Plan *
ensure_collocation(PlannerInfo *root,
				   Plan *result_plan,
				   List *tlist,
				   List *group_pathkeys,
				   List **current_pathkeys,
				   CdbPathLocus *current_locus)
{
	Query	   *parse = root->parse;
	double		motion_cost_per_row =
	(gp_motion_cost_per_row > 0.0) ?
	gp_motion_cost_per_row :
	2.0 * cpu_tuple_cost;

	/*
	 * Each group in a GROUP BY needs to be handled in the same segment.
	 * Redistribute the data if needed.
	 */
	if (group_pathkeys && !CdbPathLocus_IsGeneral(*current_locus))
	{
		if (!cdbpathlocus_collocates(root, *current_locus, group_pathkeys, false))
		{
			List	   *dist_exprs = NIL;
			ListCell   *lc;
			bool		hashable = true;

			foreach (lc, parse->groupClause)
			{
				SortGroupClause *sc = (SortGroupClause *) lfirst(lc);
				TargetEntry *tle = get_sortgroupclause_tle(sc, tlist);

				if (!isGreenplumDbHashable(exprType((Node *) tle->expr)))
				{
					hashable = false;
					break;
				}

				dist_exprs = lappend(dist_exprs, tle->expr);
			}

			if (hashable)
			{
				result_plan = (Plan *) make_motion_hash(root, result_plan, dist_exprs);
				result_plan->total_cost += motion_cost_per_row * result_plan->plan_rows;
				*current_pathkeys = NIL; /* no longer sorted */
				Assert(result_plan->flow);

				/*
				 * Change current_locus based on the new distribution
				 * pathkeys.
				 */
				CdbPathLocus_MakeHashed(current_locus, group_pathkeys);
			}
			else
			{
				if (result_plan->flow->flotype != FLOW_SINGLETON)
					result_plan =
						(Plan *) make_motion_gather_to_QE(root, result_plan, *current_pathkeys);
			}
		}
	}
	/*
	 * If there is no GROUP BY (or a dummy GROUP by with a constant), must
	 * gather everything to single node.
	 */
	else
	{
		if (!CdbPathLocus_IsGeneral(*current_locus) &&
			result_plan->flow->flotype != FLOW_SINGLETON)
		{
			result_plan =
				(Plan *) make_motion_gather_to_QE(root, result_plan, *current_pathkeys);
		}
	}

	return result_plan;
}

static void
make_two_stage_aggref_targetlists(PlannerInfo *root,
								  List *orig_tlist,
								  List *orig_havingQual,
								  WindowFuncLists *wflists,
								  List **partial_tlist,
								  List **final_tlist,
								  List **final_havingQual)
{
	Query	   *parse = root->parse;
	ListCell   *lc;
	make_partial_agg_targetlist_context cxt;

	*partial_tlist = NIL;
	*final_tlist = NIL;

	cxt.root = root;
	cxt.partial_tlist = NIL;
	cxt.found_aggrefs = false;

	foreach(lc, orig_tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		TargetEntry *final_tle;
		Expr	   *expr;

		cxt.found_aggrefs = false;
		expr = (Expr *) replace_stage_aggrefs_mutator((Node *) tle->expr, &cxt);

		/*
		 * If the entry didn't contain any Aggrefs, add it to the lower Agg's
		 * targetlist as is. It will be passed through to the final agg.
		 * (These are GROUP BY expressions, they would not be allowed otherwise.).
		 */
		if (!cxt.found_aggrefs)
		{
			if (!parse->hasWindowFuncs || !contain_windowfuncs((Node *) expr))
			{
				TargetEntry *partial_tle;

				partial_tle = makeTargetEntry(expr,
											  list_length(cxt.partial_tlist) + 1,
											  tle->resname,
											  tle->resjunk);
				cxt.partial_tlist = lappend(cxt.partial_tlist, partial_tle);
			}
		}

		final_tle = makeTargetEntry(expr,
									list_length(*final_tlist) + 1,
									tle->resname,
									tle->resjunk);
		final_tle->ressortgroupref = tle->ressortgroupref;
		final_tle->resorigtbl = tle->resorigtbl;
		final_tle->resorigcol = tle->resorigcol;

		*final_tlist = lappend(*final_tlist, final_tle);
	}

	/*
	 * We're almost done. But because we modified the final target list, we must
	 * modify all expressions that we use after this in the planner accordingly.
	 */

	/* Also mutate havingQual. It usually also contains Aggrefs. */
	*final_havingQual = (List *) replace_stage_aggrefs_mutator((Node *) orig_havingQual, &cxt);

	/* And in WindowFuncs */
	if (wflists)
	{
		int			i;

		for (i = 0; i < wflists->maxWinRef; i++)
		{
			wflists->windowFuncs[i] = (List *)
				replace_stage_aggrefs_mutator((Node *) wflists->windowFuncs[i], &cxt);
		}
	}

	/*
	 * We need to also pass through any Vars referred by WindowClause frames
	 */
	root->parse->windowClause = (List *)
		replace_stage_aggrefs_mutator((Node *) root->parse->windowClause,
									  &cxt);
	List	   *extravars = pull_var_clause((Node *) root->parse->windowClause, true);
	foreach (lc, extravars)
	{
		Expr	   *var = (Expr *) lfirst(lc);

		if (IsA(var, Var))
		{
			if (!tlist_member_ignore_relabel((Node *) var, cxt.partial_tlist))
			{
				TargetEntry *tle;

				tle = makeTargetEntry(var,
									  list_length(cxt.partial_tlist) + 1,
									  NULL,
									  tle->resjunk);
				cxt.partial_tlist = lappend(cxt.partial_tlist, tle);

				/* Also pass these through the final agg */

				tle = makeTargetEntry(var,
									  list_length(*final_tlist) + 1,
									  NULL,
									  tle->resjunk);
				*final_tlist = lappend(*final_tlist, tle);
			}
		}
	}

	/*
	 * The plan is now ready. But because we modified the final target list,
	 * must whack the eclasses accordingly. This is the same hack that
	 * optimize_minmax_aggregates() does.
	 */
	mutate_eclass_expressions(root,
							  replace_stage_aggrefs_mutator,
							  &cxt);

	*partial_tlist = cxt.partial_tlist;

	//elog(NOTICE, "orig_tlist   : %s", nodeToString(orig_tlist));
	//elog(NOTICE, "partial_tlist: %s", nodeToString(*partial_tlist));
	//elog(NOTICE, "final_tlist  : %s", nodeToString(*final_tlist));
}

static Node *
replace_stage_aggrefs_mutator(Node *node, make_partial_agg_targetlist_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Aggref))
	{
		Aggref	   *orig_aggref = (Aggref *) node;

		if (orig_aggref->agglevelsup == 0)
		{
			Aggref	   *partial_aggref;
			Aggref	   *final_aggref;
			Oid			transtype;
			Oid			inputTypes[FUNC_MAX_ARGS];
			int			nargs;
			int			partialAggrefId;
			ListCell   *lc;

			/* Get type information for the Aggref */
			transtype = lookup_agg_transtype(orig_aggref);
			nargs = get_aggregate_argtypes(orig_aggref, inputTypes);
			transtype = resolve_aggregate_transtype(orig_aggref->aggfnoid,
													transtype,
													inputTypes,
													nargs);

			Assert(orig_aggref->aggstage == AGGSTAGE_NORMAL);
			/* two-stage aggregation not supported for ordered-set aggs */
			Assert(!orig_aggref->aggdirectargs);
			Assert(!orig_aggref->aggorder);

			partial_aggref = makeNode(Aggref);
			partial_aggref->aggfnoid = orig_aggref->aggfnoid;
			partial_aggref->aggtype = transtype;
			partial_aggref->aggdirectargs = NIL;
			partial_aggref->args = copyObject(orig_aggref->args);
			partial_aggref->aggorder = NULL;
			partial_aggref->aggdistinct = copyObject(orig_aggref->aggdistinct);
			partial_aggref->aggfilter = copyObject(orig_aggref->aggfilter);
			partial_aggref->aggstar = orig_aggref->aggstar;
			partial_aggref->aggvariadic = orig_aggref->aggvariadic;
			partial_aggref->aggkind = orig_aggref->aggkind;
			partial_aggref->agglevelsup = 0;
			partial_aggref->location = orig_aggref->location;

			partial_aggref->aggstage = AGGSTAGE_PARTIAL;

			partialAggrefId = -1;

			/*
			 * Add it to the lower level's target list, if it doesn't exist yet.
			 */
			foreach (lc, context->partial_tlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);

				if (IsA(tle->expr, Aggref))
				{
					Aggref *a = (Aggref *) tle->expr;

					partial_aggref->aggpartialid = a->aggpartialid;
					if (equal(partial_aggref, a))
					{
						partialAggrefId = a->aggpartialid;
						break;
					}
				}
			}
			if (partialAggrefId == -1)
			{
				TargetEntry *tle;

				partialAggrefId = ++context->root->glob->lastPartialAggId;
				tle = makeTargetEntry((Expr *) partial_aggref,
								  list_length(context->partial_tlist) + 1,
								  pstrdup("partial agg"),
								  false);
				context->partial_tlist = lappend(context->partial_tlist, tle);
			}
			partial_aggref->aggpartialid = partialAggrefId;

			/* Now construct a new Aggref for the final aggregate */
			final_aggref = makeNode(Aggref);
			final_aggref->aggfnoid = orig_aggref->aggfnoid;
			final_aggref->aggtype = orig_aggref->aggtype;
			final_aggref->aggdirectargs = NIL;

			/*
			 * Replace the arguments with the partial_agg.
			 *
			 * It's not normally possible to have an aggregate within an
			 * aggregate, but the inner Aggref will be replaced with a Var
			 * that refers to the node below, by set_plan_references()
			 */
			final_aggref->args = NIL;
			final_aggref->aggorder = NIL;
			final_aggref->aggdistinct = NIL;
			final_aggref->aggfilter = NULL;		/* filtering is done in the partial stage */
			final_aggref->aggstar = false;
			final_aggref->aggvariadic = false;
			final_aggref->aggkind = orig_aggref->aggkind;
			final_aggref->agglevelsup = 0;
			final_aggref->location = orig_aggref->location;

			final_aggref->aggstage = AGGSTAGE_FINAL;
			final_aggref->aggpartialid = partialAggrefId;

			context->found_aggrefs = true;

			return (Node *) final_aggref;
		}
	}
	Assert(!IsA(node, SubLink));
	return expression_tree_mutator(node, replace_stage_aggrefs_mutator,
								   (void *) context);
}

/*
 * Function: cdbpathlocus_collocates
 *
 * Is a relation with the given locus guaranteed to collocate tuples with
 * non-distinct values of the key.  The key is a list of PathKeys.
 *
 * Collocation is guaranteed if the locus specifies a single process or
 * if the result is partitioned on a subset of the keys that must be
 * collocated.
 *
 * We ignore other sorts of collocation, e.g., replication or partitioning
 * on a range since these cannot occur at the moment (MPP 2.3).
 */
bool
cdbpathlocus_collocates(PlannerInfo *root, CdbPathLocus locus, List *pathkeys,
						bool exact_match)
{
	ListCell   *i;
	List	   *pk_eclasses;

	if (CdbPathLocus_IsBottleneck(locus))
		return true;

	if (!CdbPathLocus_IsHashed(locus))
		return false;  /* Or would HashedOJ ok, too? */

	if (exact_match && list_length(pathkeys) != list_length(locus.partkey_h))
		return false;

	/*
	 * Extract a list of eclasses from the pathkeys.
	 */
	pk_eclasses = NIL;
	foreach(i, pathkeys)
	{
		PathKey	   *pathkey = (PathKey *) lfirst(i);
		EquivalenceClass *ec;

		ec = pathkey->pk_eclass;
		while (ec->ec_merged != NULL)
			ec = ec->ec_merged;

		pk_eclasses = lappend(pk_eclasses, ec);
	}

	/*
	 * Check for containment of locus in pk_eclasses.
	 *
	 * We ignore constants in the locus hash key. A constant has the same
	 * value everywhere, so it doesn't affect collocation.
	 */
	return cdbpathlocus_is_hashed_on_eclasses(locus, pk_eclasses, true);
}


/*
 * Function: cdbpathlocus_from_flow
 *
 * Generate a locus from a flow.  Since the information needed to produce
 * canonical path keys is unavailable, this function will never return a
 * hashed locus.
 */
CdbPathLocus cdbpathlocus_from_flow(Flow *flow)
{
    CdbPathLocus    locus;

    CdbPathLocus_MakeNull(&locus);

    if (!flow)
        return locus;

    switch (flow->flotype)
    {
        case FLOW_SINGLETON:
            if (flow->segindex == -1)
                CdbPathLocus_MakeEntry(&locus);
            else
                CdbPathLocus_MakeSingleQE(&locus);
            break;
        case FLOW_REPLICATED:
            CdbPathLocus_MakeReplicated(&locus);
            break;
        case FLOW_PARTITIONED:
			CdbPathLocus_MakeStrewn(&locus);
            break;
        case FLOW_UNDEFINED:
        default:
            Insist(0);
    }
    return locus;
}

/* Function lookup_agg_transtype
 *
 * Return the transition type Oid of the given aggregate fuction or throw
 * an error, if none.
 */
static Oid
lookup_agg_transtype(Aggref *aggref)
{
	Oid			aggid = aggref->aggfnoid;
	Oid			result;
	HeapTuple	tuple;

	/* XXX: would have been get_agg_transtype() */
	tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggid));
	if (!tuple)
		elog(ERROR, "cache lookup failed for aggregate %u", aggid);

	result = ((Form_pg_aggregate) GETSTRUCT(tuple))->aggtranstype;

	ReleaseSysCache(tuple);

	return result;
}

/**
 * Update the scatter clause before a query tree's targetlist is about to
 * be modified. The scatter clause of a query tree will correspond to
 * old targetlist entries. If the query tree is modified and the targetlist
 * is to be modified, we must call this method to ensure that the scatter clause
 * is kept in sync with the new targetlist.
 */
void UpdateScatterClause(Query *query, List *newtlist)
{
	Assert(query);
	Assert(query->targetList);
	Assert(newtlist);

	if (query->scatterClause
			&& list_nth(query->scatterClause, 0) != NULL /* scattered randomly */
			)
	{
		Assert(list_length(query->targetList) == list_length(newtlist));
		List *scatterClause = NIL;
		ListCell *lc = NULL;
		foreach (lc, query->scatterClause)
		{
			Expr *o = (Expr *) lfirst(lc);
			Assert(o);
			TargetEntry *tle = tlist_member((Node *) o, query->targetList);
			Assert(tle);
			TargetEntry *ntle = list_nth(newtlist, tle->resno - 1);
			scatterClause = lappend(scatterClause, copyObject(ntle->expr));
		}
		query->scatterClause = scatterClause;
	}
}
