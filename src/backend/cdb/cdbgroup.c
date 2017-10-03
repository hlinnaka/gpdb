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
