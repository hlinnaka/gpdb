/*-------------------------------------------------------------------------
 *
 * cdbdistinctpaths.c
 *	  Routines to aid in planning DISTINCT queries for parallel
 *    execution.  These functions are called from create_distinct_paths()
 *	  in planner.c, they're kept here to keep the upstream function's size
 *	  reasonable.
 *
 * Portions Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbgroupingpaths.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbdistinctpaths.h"
#include "cdb/cdbgroup.h"
#include "cdb/cdbpath.h"
#include "cdb/cdbutil.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/tlist.h"
#include "utils/selfuncs.h"

void
cdb_init_distinct_path_generation(PlannerInfo *root, double numDistinctRows,
								  MppDistinctInfo *dinfo)
{
	Query	   *parse = root->parse;

	/*
	 * MPP: If there's a DISTINCT clause and we're not collocated on the
	 * distinct key, we need to redistribute on that key.  In addition, we
	 * need to consider whether to "pre-unique" by doing a Sort-Unique
	 * operation on the data as currently distributed, redistributing on the
	 * district key, and doing the Sort-Unique again. This 2-phase approach
	 * will be a win, if the cost of redistributing the entire input exceeds
	 * the cost of an extra Redistribute-Sort-Unique on the pre-uniqued
	 * (reduced) input.
	 */
	make_distribution_exprs_for_groupclause(root,
											parse->distinctClause,
			make_tlist_from_pathtarget(root->upper_targets[UPPERREL_WINDOW]),
											&dinfo->distinct_dist_pathkeys,
											&dinfo->distinct_dist_exprs,
											&dinfo->distinct_dist_opfamilies,
											&dinfo->distinct_dist_sortrefs);

	if (parse->hasDistinctOn || !enable_hashagg)
		dinfo->allow_hash_preunique = false;
	else
		dinfo->allow_hash_preunique = true;
}

void
create_distinct_sorted_paths(PlannerInfo *root,
							 MppDistinctInfo *dinfo,
							 RelOptInfo *distinct_rel,
							 Path *input_path)
{
	/* pre-sorted input */
	CdbPathLocus locus;
	Path	   *path;

	if (cdbpathlocus_collocates_pathkeys(root, input_path->locus,
										 dinfo->distinct_dist_pathkeys,
										 false /* exact_match */ ))
	{
		add_path(distinct_rel, (Path *)
				 create_upper_unique_path(root, distinct_rel,
										  input_path,
										  list_length(root->distinct_pathkeys),
										  dinfo->numDistinctRows));
		return;
	}

	if (dinfo->distinct_dist_exprs)
	{
		locus = cdbpathlocus_from_exprs(root,
										dinfo->distinct_dist_exprs,
										dinfo->distinct_dist_opfamilies,
										dinfo->distinct_dist_sortrefs,
										getgpsegmentCount());
	}
	else
	{
		CdbPathLocus_MakeSingleQE(&locus, getgpsegmentCount());
	}

	/* construct path for no-preunique */
	path = cdbpath_create_motion_path(root,
									  input_path,
									  input_path->pathkeys,
									  false,
									  locus);
	path = (Path *) create_upper_unique_path(root, distinct_rel,
											 path,
											 list_length(root->distinct_pathkeys),
											 dinfo->numDistinctRows);
	add_path(distinct_rel, path);

	/* Add path with pre-Unique */
	/*
	 * Opportunistically remove duplicates before sending the rows. This
	 * is pretty cheap, because the input is already in order, so do it
	 * always. We still need a Unique on top of the Motion, to remove
	 * duplicates arising from different segments.
	 */
	path = (Path *) create_upper_unique_path(root, distinct_rel,
											 input_path,
											 list_length(root->distinct_pathkeys),
											 dinfo->numDistinctRows);
	path = cdbpath_create_motion_path(root,
									  path,
									  path->pathkeys,
									  false,
									  locus);
	path = (Path *) create_upper_unique_path(root, distinct_rel,
											 path,
											 list_length(root->distinct_pathkeys),
											 dinfo->numDistinctRows);
	add_path(distinct_rel, path);
}

void
create_distinct_sort_paths(PlannerInfo *root,
						   MppDistinctInfo *dinfo,
						   RelOptInfo *distinct_rel,
						   List *needed_pathkeys,
						   Path *input_path)
{
	/* unsorted input, generate paths that Sort it. */
	CdbPathLocus locus;
	Path	   *path;

	if (cdbpathlocus_collocates_pathkeys(root, input_path->locus,
										 dinfo->distinct_dist_pathkeys,
										 false /* exact_match */ ))
	{
		path = (Path *) create_sort_path(root, distinct_rel,
										 input_path,
										 needed_pathkeys,
										 -1.0);
		path = (Path *) create_upper_unique_path(root, distinct_rel,
												 path,
												 list_length(root->distinct_pathkeys),
												 dinfo->numDistinctRows);
		add_path(distinct_rel, path);
		return;
	}

	if (dinfo->distinct_dist_exprs)
	{
		locus = cdbpathlocus_from_exprs(root,
										dinfo->distinct_dist_exprs,
										dinfo->distinct_dist_opfamilies,
										dinfo->distinct_dist_sortrefs,
										getgpsegmentCount());
	}
	else
	{
		CdbPathLocus_MakeSingleQE(&locus, getgpsegmentCount());
	}

	/* no-preunique */
	path = (Path *) cdbpath_create_motion_path(root,
											   input_path,
											   input_path->pathkeys,
											   false,
											   locus);
	path = (Path *) create_sort_path(root, distinct_rel,
									 path,
									 needed_pathkeys,
									 -1.0);
	path = (Path *) create_upper_unique_path(root, distinct_rel,
											 path,
											 list_length(root->distinct_pathkeys),
											 dinfo->numDistinctRows);
	add_path(distinct_rel, path);

	/* Add path with Sort+Unique pre-Unique */
	path = (Path *) create_sort_path(root, distinct_rel,
									 input_path,
									 needed_pathkeys,
									 -1.0);
	path = (Path *) create_upper_unique_path(root, distinct_rel,
											 path,
											 list_length(root->distinct_pathkeys),
											 dinfo->numDistinctRows);
	path = cdbpath_create_motion_path(root,
									  path,
									  path->pathkeys,
									  false,
									  locus);
	/*
	 * If this was a Gather motion, the Motion should've preserved the order,
	 * and we don't need a sort again.
	 */
	if (!CdbPathLocus_IsBottleneck(locus))
		path = (Path *) create_sort_path(root, distinct_rel,
										 path,
										 needed_pathkeys,
										 -1.0);
	path = (Path *) create_upper_unique_path(root, distinct_rel,
											 path,
											 list_length(root->distinct_pathkeys),
											 dinfo->numDistinctRows);
	add_path(distinct_rel, path);

	/* Add path with streaming HashAgg as pre-Unique */
	path = (Path *) create_sort_path(root, distinct_rel,
									 input_path,
									 needed_pathkeys,
									 -1.0);
	path = (Path *) create_upper_unique_path(root, distinct_rel,
											 path,
											 list_length(root->distinct_pathkeys),
											 dinfo->numDistinctRows);
	path = cdbpath_create_motion_path(root,
									  path,
									  path->pathkeys,
									  false,
									  locus);
	path = (Path *) create_sort_path(root, distinct_rel,
									 path,
									 needed_pathkeys,
									 -1.0);
	path = (Path *) create_upper_unique_path(root, distinct_rel,
											 path,
											 list_length(root->distinct_pathkeys),
											 dinfo->numDistinctRows);
	add_path(distinct_rel, path);
}

void
create_distinct_hashed_paths(PlannerInfo *root,
							 MppDistinctInfo *dinfo,
							 RelOptInfo *distinct_rel,
							 Path *input_path,
							 struct HashAggTableSizes *hash_info)
{
	/* pre-sorted input */
	Query	   *parse = root->parse;
	CdbPathLocus locus;
	Path	   *path;

	if (cdbpathlocus_collocates_pathkeys(root, input_path->locus,
										 dinfo->distinct_dist_pathkeys,
										 false /* exact_match */ ))
	{
		path = (Path *) create_agg_path(root,
										distinct_rel,
										input_path,
										input_path->pathtarget,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										parse->distinctClause,
										NIL,
										NULL,
										dinfo->numDistinctRows,
										hash_info);
		add_path(distinct_rel, path);
		return;
	}

	if (dinfo->distinct_dist_exprs)
	{
		locus = cdbpathlocus_from_exprs(root,
										dinfo->distinct_dist_exprs,
										dinfo->distinct_dist_opfamilies,
										dinfo->distinct_dist_sortrefs,
										getgpsegmentCount());
	}
	else
	{
		CdbPathLocus_MakeSingleQE(&locus, getgpsegmentCount());
	}

	/* add path for no-preunique */
	path = cdbpath_create_motion_path(root,
									  input_path,
									  input_path->pathkeys,
									  false,
									  locus);
	path = (Path *) create_agg_path(root,
									distinct_rel,
									path,
									path->pathtarget,
									AGG_HASHED,
									AGGSPLIT_SIMPLE,
									false, /* streaming */
									parse->distinctClause,
									NIL,
									NULL,
									dinfo->numDistinctRows,
									hash_info);
	add_path(distinct_rel, path);

	/* Add path with streaming HashAgg pre-Unique */
	path = (Path *) create_agg_path(root,
									distinct_rel,
									input_path,
									input_path->pathtarget,
									AGG_HASHED,
									AGGSPLIT_SIMPLE,
									true, /* streaming */
									parse->distinctClause,
									NIL,
									NULL, /* aggcosts */
									estimate_num_groups_across_segments(dinfo->numDistinctRows,
																		path->rows,
																		getgpsegmentCount()),
									NULL);
	path = cdbpath_create_motion_path(root,
									  path,
									  path->pathkeys,
									  false,
									  locus);
	path = (Path *) create_agg_path(root,
									distinct_rel,
									path,
									path->pathtarget,
									AGG_HASHED,
									AGGSPLIT_SIMPLE,
									false, /* streaming */
									parse->distinctClause,
									NIL,
									NULL,
									dinfo->numDistinctRows,
									hash_info);
	add_path(distinct_rel, path);

	/*
	 * We don't consider using a Sort+Unique to deduplicated before the motion.
	 * Seems unlikely to be a win.
	 */
}
