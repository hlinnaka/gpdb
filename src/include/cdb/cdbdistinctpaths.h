/*-------------------------------------------------------------------------
 *
 * cdbdistinctpaths.h
 *	  prototypes for cdbdistinctpaths.c.
 *
 *
 * Portions Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbdistinctpaths.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDISTINCTPATHS_H
#define CDBDISTINCTPATHS_H

#include "nodes/relation.h"
#include "optimizer/pathnode.h"

typedef struct
{
	List	   *distinctClause;
	double		numDistinctRows;
	List	   *distinct_dist_pathkeys;
	List	   *distinct_dist_exprs;
	List	   *distinct_dist_opfamilies;
	List	   *distinct_dist_sortrefs;
	bool		allow_hash_preunique;
} MppDistinctInfo;

extern void cdb_init_distinct_path_generation(PlannerInfo *root, double numDistinctRows,
											  MppDistinctInfo *distinct_dist_info);

extern void create_distinct_sorted_paths(PlannerInfo *root,
										 MppDistinctInfo *distinfo,
										 RelOptInfo *distinct_rel,
										 Path *input_path);

extern void create_distinct_sort_paths(PlannerInfo *root,
									   MppDistinctInfo *distinfo,
									   RelOptInfo *distinct_rel,
									   List *needed_pathkeys,
									   Path *input_path);

extern void create_distinct_hashed_paths(PlannerInfo *root,
										 MppDistinctInfo *distinfo,
										 RelOptInfo *distinct_rel,
										 Path *input_path,
										 struct HashAggTableSizes *hash_info);

#endif   /* CDBDISTINCTPATHS_H */
