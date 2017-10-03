/*-------------------------------------------------------------------------
 *
 * planmain.h
 *	  prototypes for various files in optimizer/plan
 *
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/optimizer/planmain.h,v 1.113 2008/10/04 21:56:55 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANMAIN_H
#define PLANMAIN_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "utils/uri.h"

/* GUC parameters */
#define DEFAULT_CURSOR_TUPLE_FRACTION 1.0 /* assume all rows will be fetched */
extern double cursor_tuple_fraction;

/*
 * prototypes for plan/planmain.c
 */
extern void query_planner(PlannerInfo *root, List *tlist,
			  double tuple_fraction, double limit_tuples,
			  Path **cheapest_path, Path **sorted_path,
			  double *num_groups);

/*
 * prototypes for plan/planagg.c
 */
extern Plan *optimize_minmax_aggregates(PlannerInfo *root, List *tlist,
						   Path *best_path);

/*
 * prototypes for plan/createplan.c
 */
extern Plan *create_plan(PlannerInfo *root, Path *path);
extern SubqueryScan *make_subqueryscan(PlannerInfo *root, List *qptlist, List *qpqual,
				  Index scanrelid, Plan *subplan, List *subrtable);
extern Append *make_append(List *appendplans, bool isTarget, List *tlist);
extern RecursiveUnion *make_recursive_union(List *tlist,
			   Plan *lefttree, Plan *righttree, int wtParam);
extern Sort *make_sort_from_pathkeys(PlannerInfo *root, Plan *lefttree,
						List *pathkeys, double limit_tuples, bool add_keys_to_targetlist);
extern Sort *make_sort_from_sortclauses(PlannerInfo *root, List *sortcls,
						   Plan *lefttree);
extern Sort *make_sort_from_groupcols(PlannerInfo *root, List *groupcls,
									  AttrNumber *grpColIdx,
									  Plan *lefttree);
extern List *reconstruct_group_clause(List *orig_groupClause, List *tlist,
						 AttrNumber *grpColIdx, int numcols);

extern Motion *make_motion(PlannerInfo *root, Plan *lefttree, List *sortPathKeys, bool useExecutorVarFormat);

extern Agg *make_agg(PlannerInfo *root, List *tlist, List *qual,
					 AggStrategy aggstrategy, bool streaming,
					 int numGroupCols, AttrNumber *grpColIdx, Oid *grpOperators,
					 long numGroups,
					 int numAggs,
					 Plan *lefttree);
extern HashJoin *make_hashjoin(List *tlist,
			  List *joinclauses, List *otherclauses,
			  List *hashclauses, List *hashqualclauses,
			  Plan *lefttree, Plan *righttree,
			  JoinType jointype);
extern Hash *make_hash(Plan *lefttree);
extern NestLoop *make_nestloop(List *tlist,
							   List *joinclauses, List *otherclauses,
							   Plan *lefttree, Plan *righttree,
							   JoinType jointype);
extern MergeJoin *make_mergejoin(List *tlist,
			   List *joinclauses, List *otherclauses,
			   List *mergeclauses,
			   Oid *mergefamilies,
			   int *mergestrategies,
			   bool *mergenullsfirst,
			   Plan *lefttree, Plan *righttree,
			   JoinType jointype);
extern WindowAgg *make_windowagg(PlannerInfo *root, List *tlist,
			   List *windowFuncs, Index winref,
			   int partNumCols, AttrNumber *partColIdx, Oid *partOperators,
			   int ordNumCols, AttrNumber *ordColIdx, Oid *ordOperators,
			   AttrNumber firstOrderCol, Oid firstOrderCmpOperator, bool firstOrderNullsFirst,
			   int frameOptions, Node *startOffset, Node *endOffset,
			   Plan *lefttree);
extern Material *make_material(Plan *lefttree);
extern Plan *materialize_finished_plan(PlannerInfo *root, Plan *subplan);
extern Unique *make_unique(Plan *lefttree, List *distinctList);
extern Limit *make_limit(Plan *lefttree, Node *limitOffset, Node *limitCount,
		   int64 offset_est, int64 count_est);
extern SetOp *make_setop(SetOpCmd cmd, Plan *lefttree,
		   List *distinctList, AttrNumber flagColIdx);
extern Result *make_result(PlannerInfo *root, List *tlist,
			Node *resconstantqual, Plan *subplan);
extern Repeat *make_repeat(List *tlist,
						   List *qual,
						   Expr *repeatCountExpr,
						   uint64 grouping,
						   Plan *subplan);
extern bool is_projection_capable_plan(Plan *plan);
extern Plan *add_sort_cost(PlannerInfo *root, Plan *input, 
						   int numCols, 
						   AttrNumber *sortColIdx, Oid *sortOperators,
						   double limit_tuples);
extern Plan *add_agg_cost(PlannerInfo *root, Plan *plan, 
		 List *tlist, List *qual,
		 AggStrategy aggstrategy, 
		 bool streaming, 
		 int numGroupCols, AttrNumber *grpColIdx,
		 long numGroups,
		 int numAggs);
extern Plan *plan_pushdown_tlist(PlannerInfo *root, Plan *plan, List *tlist);      /*CDB*/

extern List *create_external_scan_uri_list(struct ExtTableEntry *extEntry, bool *ismasteronly);

/*
 * prototypes for plan/initsplan.c
 */
extern int	from_collapse_limit;
extern int	join_collapse_limit;

extern void add_base_rels_to_query(PlannerInfo *root, Node *jtnode);
extern void build_base_rel_tlists(PlannerInfo *root, List *final_tlist);
extern void add_vars_to_targetlist(PlannerInfo *root, List *vars,
					   Relids where_needed);
extern List *deconstruct_jointree(PlannerInfo *root);
extern void distribute_restrictinfo_to_rels(PlannerInfo *root,
								RestrictInfo *restrictinfo);
extern void process_implied_equality(PlannerInfo *root,
						 Oid opno,
						 Expr *item1,
						 Expr *item2,
						 Relids qualscope,
						 Relids nullable_relids,
						 bool below_outer_join,
						 bool both_const);
extern RestrictInfo *build_implied_join_equality(Oid opno,
							Expr *item1,
							Expr *item2,
							Relids qualscope,
							Relids nullable_relids);

/*
 * prototypes for plan/setrefs.c
 */
extern Plan *set_plan_references(PlannerGlobal *glob,
					Plan *plan,
					List *rtable);
extern List *set_returning_clause_references(PlannerGlobal *glob,
								List *rlist,
								Plan *topplan,
								Index resultRelation);

extern void extract_query_dependencies(List *queries,
						   List **relationOids,
						   List **invalItems);
extern void fix_opfuncids(Node *node);
extern void set_opfuncid(OpExpr *opexpr);
extern void set_sa_opfuncid(ScalarArrayOpExpr *opexpr);
extern void record_plan_function_dependency(PlannerGlobal *glob, Oid funcid);
extern void extract_query_dependencies(List *queries,
									   List **relationOids,
									   List **invalItems);
extern void cdb_extract_plan_dependencies(PlannerGlobal *glob, Plan *plan);

#endif   /* PLANMAIN_H */
