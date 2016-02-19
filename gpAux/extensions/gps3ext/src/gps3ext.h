#ifndef _GPS3EXT_H_
#define _GPS3EXT_H_

// TODO include GpId from proper place
typedef struct GpId {
    int4 numsegments; /* count of distinct segindexes */
    int4 dbid;        /* the dbid of this database */
    int4 segindex;    /* content indicator: -1 for entry database,
                       * 0, ..., n-1 for segment database *
                       * a primary and its mirror have the same segIndex */
} GpId;
extern GpId GpIdentity;

#endif
