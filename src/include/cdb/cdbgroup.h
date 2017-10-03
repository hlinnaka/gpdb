/*-------------------------------------------------------------------------
 *
 * cdbgroup.h
 *	  prototypes for cdbgroup.c.
 *
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbgroup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBGROUP_H
#define CDBGROUP_H

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"

extern void UpdateScatterClause(Query *query, List *newtlist);

extern bool cdbpathlocus_collocates(PlannerInfo *root, CdbPathLocus locus, List *pathkeys, bool exact_match);
extern CdbPathLocus cdbpathlocus_from_flow(Flow *flow);

#endif   /* CDBGROUP_H */
