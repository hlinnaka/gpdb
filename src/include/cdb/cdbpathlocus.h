/*-------------------------------------------------------------------------
 *
 * cdbpathlocus.h
 *
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbpathlocus.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBPATHLOCUS_H
#define CDBPATHLOCUS_H

#include "nodes/pg_list.h"      /* List */
#include "nodes/bitmapset.h"    /* Bitmapset */

struct Plan;                    /* defined in plannodes.h */
struct RelOptInfo;              /* defined in relation.h */
struct PlannerInfo;				/* defined in relation.h */
struct Flow;


/*
 * CdbLocusType
 */
typedef enum CdbLocusType
{
    CdbLocusType_Null,
    CdbLocusType_Entry,         /* a single backend process on the entry db:
                                 * usually the qDisp itself, but could be a
                                 * qExec started by the entry postmaster.
                                 */
    CdbLocusType_SingleQE,      /* a single backend process on any db: the
                                 * qDisp itself, or a qExec started by a
                                 * segment postmaster or the entry postmaster.
                                 */
    CdbLocusType_General,       /* compatible with any locus (data is
                                 * self-contained in the query plan or
                                 * generally available in any qExec or qDisp) */
    CdbLocusType_SegmentGeneral,/* generally available in any qExec, but not
				 * available in qDisp */
    CdbLocusType_Replicated,    /* replicated over all qExecs of an N-gang */
    CdbLocusType_Hashed,        /* hash partitioned over all qExecs of N-gang */
    CdbLocusType_Strewn,        /* partitioned on no known function */
    CdbLocusType_End            /* = last valid CdbLocusType + 1 */
} CdbLocusType;

#define CdbLocusType_IsValid(locustype)     \
    ((locustype) > CdbLocusType_Null &&     \
     (locustype) < CdbLocusType_End)

#define CdbLocusType_IsValidOrNull(locustype)   \
    ((locustype) >= CdbLocusType_Null &&        \
     (locustype) < CdbLocusType_End)

/*
 * CdbPathLocus
 *
 * Specifies a distribution of tuples across processes.
 *
 * If the distribution is a hashed one (locustype == CdbLocusType_Hashed),
 *      then the tuples are distributed based on values of a partitioning
 *		key. The partitioning key is often called a "distribution key", to
 *		avoid confusion with table partitioning.
 *
 *      A tuple's partitioning key is an m-tuple of values (v1, v2, ..., vm)
 *      computed by evaluating an m-tuple of expressions (e1, e2, ..., em)
 *      on the columns of the tuple.  Each expression 'ei' is drawn from
 *      the corresponding set of expressions Ei = {expr, expr, ... }
 *      in the i'th position of the m-List (E1, E2, ..., Em) that hangs
 *      from the 'distkeys' field.  (The data structure used to represent
 *      these sets depends upon the locustype, as described below.)
 *      The CdbPathLocus_Degree() macro returns 'm'.
 *
 *      All choices of the 'ei' from the 'Ei' lead to an equivalent
 *      distribution of rows; thus the set of possible m-tuples of
 *      expressions (e1, e2, ..., em) forms an equivalence class
 *      with respect to the partitioning function.
 *
 *      We use this information primarily to optimize joins for which we
 *      have an equijoin predicate on every column of the partitioning key.
 *      Another usage is to detect if the partitioning matches grouping for
 *      a GROUP BY or similar clause, so that we can perform the grouping
 *      separately on the segments, and just gather the grouped results.
 *
 * If locustype == CdbLocusType_Hashed:
 *      Rows are distributed on a hash function of the partitioning key.
 *      The 'distkeys' field points to a DistributionKey list.
 *      Each of the sets 'Ei' is represented as a DistributionKey, and in
 *      particular its equivalence class, which is a List of expressions that
 *      are constrained by equality predicates to be equal to one another.
 *
 * If locustype == CdbLocusType_Strewn:
 *      Rows are distributed according to a criterion that is unknown or
 *      may depend on inputs that are unknown or unavailable in the present
 *      context.  The 'distkeys' field is NIL and the CdbPathLocus_Degree()
 *      macro returns 0.
 *
 * If the distribution is not partitioned, then the 'distkeys' field is NIL
 *      and the CdbPathLocus_Degree() macro returns 0.
 *
 * The attribute numsegments specify how many segments are the tuples
 * distributed on, from segment 0 to segment `numsegments-1`.  In the future
 * we might further change it to a range or list so discontinuous segments
 * can be described.  This numsegments has different meaning for different
 * locustype:
 * - Null: numsegments is usually meaningless in Null locus as it will be
 *   remade to other locus types later.  But there is also cases that we set
 *   a valid numsegments in Null locus, this value will be kept when remade
 *   it to other locus types, and it becomes meaningful after that;
 * - Entry: numsegments in Entry locus specify the candidate segments to put
 *   the Entry node on, it's master and all the primary segments in current
 *   implementation;
 * - SingleQE: numsegments in SingleQE locus specify the candidate segments
 *   to put the SingleQE node on, although SingleQE is always executed on one
 *   segment but numsegments usually have a value > 1;
 * - General: similar with Entry and SingleQE;
 * - SegmentGeneral, Replicated, Hashed, Strewn: numsegments in
 *   these locus types specify the segments that contain the tuples;
 */
typedef struct CdbPathLocus
{
	CdbLocusType locustype;
	List	   *distkeys;
	int			numsegments;
} CdbPathLocus;

#define CdbPathLocus_Degree(locus)          \
	(CdbPathLocus_IsHashed(locus) ? list_length((locus).distkeys) : 0)

#define CdbPathLocus_NumSegments(locus)         \
            ((locus).numsegments)

/*
 * CdbPathLocus_IsEqual
 *
 * Returns true if one CdbPathLocus is exactly the same as the other.
 * To test for logical equivalence, use cdbpathlocus_equal() instead.
 */
#define CdbPathLocus_IsEqual(a, b)              \
            ((a).locustype == (b).locustype &&  \
             (a).numsegments == (b).numsegments && \
             (a).distkeys == (b).distkeys)

#define CdbPathLocus_CommonSegments(a, b) \
            Min((a).numsegments, (b).numsegments)

/*
 * CdbPathLocus_IsBottleneck
 *
 * Returns true if the locus indicates confinement to a single process:
 * either a singleton qExec (1-gang) on a segment db or the entry db, or
 * the qDisp process.
 */
#define CdbPathLocus_IsBottleneck(locus)        \
            (CdbPathLocus_IsEntry(locus) ||     \
             CdbPathLocus_IsSingleQE(locus))

/*
 * CdbPathLocus_IsPartitioned
 *
 * Returns true if the locus indicates partitioning across an N-gang, such
 * that each qExec has a disjoint subset of the rows.
 */
#define CdbPathLocus_IsPartitioned(locus)       \
            (CdbPathLocus_IsHashed(locus) ||    \
             CdbPathLocus_IsStrewn(locus))

#define CdbPathLocus_IsNull(locus)          \
            ((locus).locustype == CdbLocusType_Null)
#define CdbPathLocus_IsEntry(locus)         \
            ((locus).locustype == CdbLocusType_Entry)
#define CdbPathLocus_IsSingleQE(locus)      \
            ((locus).locustype == CdbLocusType_SingleQE)
#define CdbPathLocus_IsGeneral(locus)       \
            ((locus).locustype == CdbLocusType_General)
#define CdbPathLocus_IsReplicated(locus)    \
            ((locus).locustype == CdbLocusType_Replicated)
#define CdbPathLocus_IsHashed(locus)        \
            ((locus).locustype == CdbLocusType_Hashed)
#define CdbPathLocus_IsStrewn(locus)        \
            ((locus).locustype == CdbLocusType_Strewn)
#define CdbPathLocus_IsSegmentGeneral(locus)        \
            ((locus).locustype == CdbLocusType_SegmentGeneral)

#define CdbPathLocus_MakeSimple(plocus, _locustype, numsegments_) \
    do {                                                \
        CdbPathLocus *_locus = (plocus);                \
        _locus->locustype = (_locustype);               \
        _locus->numsegments = (numsegments_);                        \
        _locus->distkeys = NIL;                       \
    } while (0)

#define CdbPathLocus_MakeNull(plocus, numsegments_)                   \
            CdbPathLocus_MakeSimple((plocus), CdbLocusType_Null, (numsegments_))
#define CdbPathLocus_MakeEntry(plocus)                  \
            CdbPathLocus_MakeSimple((plocus), CdbLocusType_Entry, GP_POLICY_ENTRY_NUMSEGMENTS)
#define CdbPathLocus_MakeSingleQE(plocus, numsegments_)               \
            CdbPathLocus_MakeSimple((plocus), CdbLocusType_SingleQE, (numsegments_))
#define CdbPathLocus_MakeGeneral(plocus, numsegments_)                \
            CdbPathLocus_MakeSimple((plocus), CdbLocusType_General, (numsegments_))
#define CdbPathLocus_MakeSegmentGeneral(plocus, numsegments_)                \
            CdbPathLocus_MakeSimple((plocus), CdbLocusType_SegmentGeneral, (numsegments_))
#define CdbPathLocus_MakeReplicated(plocus, numsegments_)             \
            CdbPathLocus_MakeSimple((plocus), CdbLocusType_Replicated, (numsegments_))
#define CdbPathLocus_MakeHashed(plocus, distkeys_, numsegments_)       \
    do {                                                \
        CdbPathLocus *_locus = (plocus);                \
        _locus->locustype = CdbLocusType_Hashed;		\
        _locus->numsegments = (numsegments_);           \
        _locus->distkeys = (distkeys_);					\
        Assert(cdbpathlocus_is_valid(*_locus));         \
    } while (0)
#define CdbPathLocus_MakeStrewn(plocus, numsegments_)                 \
            CdbPathLocus_MakeSimple((plocus), CdbLocusType_Strewn, (numsegments_))

/************************************************************************/

extern bool cdbpathlocus_equal(CdbPathLocus a, CdbPathLocus b);

/************************************************************************/

CdbPathLocus
cdbpathlocus_from_baserel(struct PlannerInfo   *root,
                          struct RelOptInfo    *rel);
CdbPathLocus
cdbpathlocus_from_exprs(struct PlannerInfo     *root,
                        List                   *hash_on_exprs,
                        int                     numsegments);
CdbPathLocus
cdbpathlocus_from_subquery(struct PlannerInfo  *root,
                           struct Plan         *subqplan,
                           Index                subqrelid);

CdbPathLocus
cdbpathlocus_join(struct RelOptInfo *joinrel, JoinType jointype, CdbPathLocus a, CdbPathLocus b);

/************************************************************************/

/*
 * cdbpathlocus_get_distkey_exprs
 *
 * Returns a List with one Expr for each distkey column.  Each item either is
 * in the given targetlist, or has no Var nodes that are not in the targetlist;
 * and uses only rels in the given set of relids.  Returns NIL if the
 * distkey cannot be expressed in terms of the given relids and targetlist.
 */
List *
cdbpathlocus_get_distkey_exprs(CdbPathLocus     locus,
                               Bitmapset       *relids,
                               List            *targetlist);

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
cdbpathlocus_pull_above_projection(struct PlannerInfo  *root,
                                   CdbPathLocus         locus,
                                   Bitmapset           *relids,
                                   List                *targetlist,
                                   List                *newvarlist,
                                   Index                newrelid);

/************************************************************************/

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
cdbpathlocus_is_hashed_on_exprs(CdbPathLocus locus, List *exprlist, bool ignore_constants);

/*
 * cdbpathlocus_is_hashed_on_eclasses
 *
 * This function tests whether grouping on a given set of exprs can be done
 * in place without motion.
 *
 * For a hashed locus, returns false if the distkey has any column whose
 * equivalence class is not in 'eclasses' list.
 *
 * If 'ignore_constants' is true, any constants in the locus are ignored.
 */
bool
cdbpathlocus_is_hashed_on_eclasses(CdbPathLocus locus, List *eclasses, bool ignore_constants);

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
cdbpathlocus_is_hashed_on_relids(CdbPathLocus locus, Bitmapset *relids);

/*
 * cdbpathlocus_is_valid
 *
 * Returns true if locus appears structurally valid.
 */
bool
cdbpathlocus_is_valid(CdbPathLocus locus);

List *
cdbpathlocus_get_distkeys_for_pathkeys(List *pathkeys);

#endif   /* CDBPATHLOCUS_H */
