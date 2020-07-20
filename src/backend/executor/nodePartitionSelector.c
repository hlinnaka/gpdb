/*-------------------------------------------------------------------------
 *
 * nodePartitionSelector.c
 *	  implement the execution of PartitionSelector for pruning partitions
 *	  based on rows seen by the inner side of a join.
 *
 * For example:
 *
 * explain (costs off, timing off, analyze)
 *   select * from t, pt where tid = ptid;
 *                                             QUERY PLAN                                                    
 * ---------------------------------------------------------------------------------------------
 *  Gather Motion 3:1  (slice1; segments: 3) (actual rows=2 loops=1)
 *    ->  Hash Join (actual rows=2 loops=1)
 *          Hash Cond: (pt_1_prt_2.ptid = t.tid)
 *          ->  Append (actual rows=2 loops=1)
 *                ->  Seq Scan on pt_1_prt_2 (actual rows=1 loops=1)
 *                ->  Seq Scan on pt_1_prt_3 (actual rows=1 loops=1)
 *                ->  Seq Scan on pt_1_prt_4 (never executed)
 *                ->  Seq Scan on pt_1_prt_5 (never executed)
 *                ->  Seq Scan on pt_1_prt_6 (never executed)
 *                ->  Seq Scan on pt_1_prt_junk_data (never executed)
 *          ->  Hash (actual rows=2 loops=1)
 *                Buckets: 524288  Batches: 1  Memory Usage: 4097kB
 *                ->  Partition Selector for pt (dynamic scan id: 1) (actual rows=2 loops=1)
 *                      ->  Broadcast Motion 3:3  (slice2; segments: 3) (actual rows=2 loops=1)
 *                            ->  Seq Scan on t (actual rows=2 loops=1)
 * (15 rows)
 *
 * In this example, the 't' table is scanned first, and the Hash table is
 * built. All the rows also pased through the Partition Selector node. For
 * each row, the Partition Selector computes the corresponding partition in
 * the 'pt' table. Based on the rows seen, the Append node can skip partitions
 * that cannot contain any matching rows.
 *
 * The Partition Selector has a PartitionPruneInfo struct that contains
 * the logic used to compute the matching partition for each input row.
 * In PostgreSQL, the partition pruning is performed entirely based on
 * constants (at planning time), or based on constants and Params (run-time
 * pruning). The pruning steps used in Partition Selectors can also
 * contain Vars referring to the columns of the outer side of the join
 * that are available at the Partition Selector.
 *
 * The Partition Selector performs the pruning and stores the result in a
 * special executor Param to make it available to the Append node. When doing
 * join pruning using a Partition Selector, the Append node doesn't perform
 * the pruning steps, but uses the pre-computed result Bitmapset. (A mix of
 * upstream-style pruning based on Params, an join pruning using a Partition
 * Seletor, is possible however).
 *
 * Copyright (c) 2014-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/executor/nodePartitionSelector.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "executor/executor.h"
#include "executor/execPartition.h"
#include "executor/nodePartitionSelector.h"
#include "partitioning/partdesc.h"
#include "utils/builtins.h"
#include "utils/rel.h"

static TupleTableSlot *ExecPartitionSelector(PlanState *pstate);

/* ----------------------------------------------------------------
 *		ExecInitPartitionSelector
 *
 *		Create the run-time state information for PartitionSelector node
 *		produced by Orca and initializes outer child if exists.
 *
 * ----------------------------------------------------------------
 */
PartitionSelectorState *
ExecInitPartitionSelector(PartitionSelector *node, EState *estate, int eflags)
{
	PartitionSelectorState *psstate;

	/* check for unsupported flags */
	Assert (!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)));

	psstate = makeNode(PartitionSelectorState);
	psstate->ps.plan = (Plan *) node;
	psstate->ps.state = estate;

	/* ExprContext initialization */
	ExecAssignExprContext(estate, &psstate->ps);

	psstate->ps.ExecProcNode = ExecPartitionSelector;

	/* tuple table initialization */
	ExecInitResultTypeTL(&psstate->ps);
	ExecAssignProjectionInfo(&psstate->ps, NULL);

	/*
	 * initialize outer plan
	 */
	outerPlanState(psstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/* Create the working data structure for pruning. */
	psstate->prune_state = ExecCreatePartitionPruneState(&psstate->ps,
														 node->part_prune_info);

	return psstate;
}

/* ----------------------------------------------------------------
 *		ExecPartitionSelector(node)
 *
 *		Compute and propagate partition table Oids that will be
 *		used by Dynamic table scan. There are two ways of
 *		executing PartitionSelector.
 *
 *		1. Constant partition elimination
 *		Plan structure:
 *			Sequence
 *				|--PartitionSelector
 *				|--DynamicSeqScan
 *		In this case, PartitionSelector evaluates constant partition
 *		constraints to compute and propagate partition table Oids.
 *		It only need to be called once.
 *
 *		2. Join partition elimination
 *		Plan structure:
 *			...:
 *				|--DynamicSeqScan
 *				|--...
 *					|--PartitionSelector
 *						|--...
 *		In this case, PartitionSelector is in the same slice as
 *		DynamicSeqScan, DynamicIndexScan or DynamicBitmapHeapScan.
 *		It is executed for each tuple coming from its child node.
 *		It evaluates partition constraints with the input tuple and
 *		propagate matched partition table Oids.
 *
 *
 * Instead of a Dynamic Table Scan, there can be other nodes that use
 * a PartSelected qual to filter rows, based on which partitions are
 * selected. Currently, ORCA uses Dynamic Table Scans, while plans
 * produced by the non-ORCA planner use gating Result nodes with
 * PartSelected quals, to exclude unwanted partitions.
 *
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecPartitionSelector(PlanState *pstate)
{
	PartitionSelectorState *node = (PartitionSelectorState *) pstate;
	PartitionSelector *ps = (PartitionSelector *) node->ps.plan;
	EState	   *estate = node->ps.state;
	ExprContext *econtext = node->ps.ps_ExprContext;
	PlanState *outerPlan = outerPlanState(node);
	TupleTableSlot *inputSlot;

	/* Join partition elimination */
	/* get tuple from outer children */
	Assert(outerPlan);
	inputSlot = ExecProcNode(outerPlan);

	if (TupIsNull(inputSlot))
	{
		/* no more tuples from outerPlan */

		/*
		 * Make sure we have an entry for this scan id in
		 * dynamicTableScanInfo. Normally, this would've been done the
		 * first time a partition is selected, but we must ensure that
		 * there is an entry even if no partitions were selected.
		 * (The traditional Postgres planner uses this method.)
		 * FIXME comment
		 */
		ParamExecData *param;

		param = &(estate->es_param_exec_vals[ps->paramid]);
		Assert(param->execPlan == NULL);
		Assert(!param->isnull);
		param->value = PointerGetDatum(node);

		return NULL;
	}

	/* partition elimination with the given input tuple */
	ResetExprContext(econtext);
	econtext->ecxt_outertuple = inputSlot;

	/*
	 * If we have a partitioning projection, project the input tuple
	 * into a tuple that looks like tuples from the partitioned table, and use
	 * selectPartitionMulti() to select the partitions. (The traditional
	 * Postgres planner uses this method.)
	 */
	node->part_prune_result = ExecAddMatchingSubPlans(node->prune_state,
													  node->part_prune_result);

	return inputSlot;
}

/* ----------------------------------------------------------------
 *		ExecReScanPartitionSelector(node)
 *
 *		ExecReScan routine for PartitionSelector.
 * ----------------------------------------------------------------
 */
void
ExecReScanPartitionSelector(PartitionSelectorState *node)
{
}

/* ----------------------------------------------------------------
 *		ExecEndPartitionSelector(node)
 *
 *		ExecEnd routine for PartitionSelector. Free resources
 *		and clear tuple.
 *
 * ----------------------------------------------------------------
 */
void
ExecEndPartitionSelector(PartitionSelectorState *node)
{
	ExecFreeExprContext(&node->ps);

	/* clean child node */
	if (NULL != outerPlanState(node))
	{
		ExecEndNode(outerPlanState(node));
	}
}

/* EOF */

