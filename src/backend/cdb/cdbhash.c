/*--------------------------------------------------------------------------
 *
 * cdbhash.c
 *	  Provides hashing routines to support consistant data distribution/location
 *    within Greenplum Database.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbhash.c
 *
 *--------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbutil.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/* Constant used to help defining upper limit for random generator */
#define UPPER_VAL ((uint32)0XA0B0C0D1)

/* Fast mod using a bit mask, assuming that y is a power of 2 */
#define FASTMOD(x,y)		((x) & ((y)-1))

/* local function declarations */
static int	ispowof2(int numsegs);
static inline int32 jump_consistent_hash(uint64 key, int32 num_segments);

/*================================================================
 *
 * HASH API FUNCTIONS
 *
 *================================================================
 */

/*
 * Create a CdbHash for this session.
 *
 * CdbHash maintains the following information about the hash.
 * In here we set the variables that should not change in the scope of the newly created
 * CdbHash, these are:
 *
 * 1 - number of segments in Greenplum Database.
 * 2 - reduction method.
 * 3 - distribution key column hash functions.
 *
 * The hash value itself will be initialized for every tuple in cdbhashinit()
 */
CdbHash *
makeCdbHash(int numsegs, int natts, Oid *hashfuncs)
{
	CdbHash    *h;
	int			i;
	bool		is_legacy_hash = false;

	Assert(numsegs > 0);		/* verify number of segments is legal. */

	if (numsegs == __GP_POLICY_EVIL_NUMSEGMENTS)
	{
		Assert(!"what's the proper value of numsegments?");
	}

	/* Allocate a new CdbHash, with space for the datatype OIDs. */
	h = palloc(sizeof(CdbHash));

	/*
	 * set this hash session characteristics.
	 */
	h->hash = 0;
	h->numsegs = numsegs;

	/*
	 * if we distribute into a relation with an empty partitioning policy, we
	 * will round robin the tuples starting off from this index. Note that the
	 * random number is created one per makeCdbHash. This means that commands
	 * that create a cdbhash object only once for all tuples (like COPY,
	 * INSERT-INTO-SELECT) behave more like a round-robin distribution, while
	 * commands that create a cdbhash per row (like INSERT) behave more like a
	 * random distribution.
	 */
	h->rrindex = cdb_randint(0, UPPER_VAL);

	/* Load hash function info */
	h->hashfuncs = (FmgrInfo *) palloc(natts * sizeof(FmgrInfo));
	for (i = 0; i < natts; i++)
	{
		Oid			funcid = hashfuncs[i];

		if (isLegacyCdbHashFunction(funcid))
			is_legacy_hash = true;

		fmgr_info(funcid, &h->hashfuncs[i]);
	}
	h->natts = natts;
	h->is_legacy_hash = is_legacy_hash;

	/*
	 * set the reduction algorithm: If num_segs is power of 2 use bit mask,
	 * else use lazy mod (h mod n)
	 */
	if (is_legacy_hash)
		h->reducealg = ispowof2(numsegs) ? REDUCE_BITMASK : REDUCE_LAZYMOD;
	else
		h->reducealg = REDUCE_JUMP_HASH;

	ereport(DEBUG4,
			(errmsg("CDBHASH hashing into %d segment databases", h->numsegs)));

	return h;
}

/*
 * Convenience routine, to create a CdbHash according to a relation's
 * distribution policy.
 */
CdbHash *
makeCdbHashForRelation(Relation rel)
{
	GpPolicy   *policy = rel->rd_cdbpolicy;
	Oid		   *hashfuncs;
	int			i;
	TupleDesc	desc = RelationGetDescr(rel);

	hashfuncs = palloc(policy->nattrs * sizeof(Oid));

	for (i = 0; i < policy->nattrs; i++)
	{
		AttrNumber	attnum = policy->attrs[i];
		Oid			typeoid = desc->attrs[attnum - 1]->atttypid;
		Oid			opfamily;

		opfamily = get_opclass_family(policy->opclasses[i]);

		hashfuncs[i] = cdb_hashproc_in_opfamily(opfamily, typeoid, false);
	}

	return makeCdbHash(policy->numsegments, policy->nattrs, hashfuncs);
}

/*
 * Initialize CdbHash for hashing the next tuple values.
 */
void
cdbhashinit(CdbHash *h)
{
	if (!h->is_legacy_hash)
		h->hash = 0;
	else
	{
		/* reset the hash value to the initial offset basis */
		h->hash = FNV1_32_INIT;
	}
}

/*
 * Add an attribute to the CdbHash calculation.
 */
void
cdbhash(CdbHash *h, int attno, Datum datum, bool isnull)
{
	uint32		hashkey = h->hash;
	uint32		hkey;

	if (!h->is_legacy_hash)
	{
		// FIXME: this assumes that we will be called for each key, in order.
		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		if (!isnull)
		{
			hkey = DatumGetUInt32(FunctionCall1(&h->hashfuncs[attno - 1], datum));
			hashkey ^= hkey;
		}
	}
	else
	{
		magic_hash_stash = hashkey;
		if (!isnull)
			hashkey = DatumGetUInt32(FunctionCall1(&h->hashfuncs[attno - 1], datum));
		else
			hashkey = hashNullDatum();
		magic_hash_stash = FNV1_32_INIT;
	}
	h->hash = hashkey;
}

/*
 * Hash a tuple of a relation with an empty policy (no hash
 * key exists) via round robin with a random initial value.
 */
void
cdbhashnokey(CdbHash *h)
{
	/* compute the hash */
	h->hash = DatumGetUInt32(hash_uint32(h->rrindex));

	h->rrindex++;				/* increment for next time around */
}

/*
 * Reduce the hash to a segment number.
 */
unsigned int
cdbhashreduce(CdbHash *h)
{
	int			result = 0;		/* TODO: what is a good initialization value?
								 * could we guarantee at this point that there
								 * will not be a negative segid in Greenplum
								 * Database and therefore initialize to this
								 * value for error checking? */

	Assert(h->reducealg == REDUCE_BITMASK ||
		   h->reducealg == REDUCE_LAZYMOD ||
		   h->reducealg == REDUCE_JUMP_HASH);

	/*
	 * Reduce our 32-bit hash value to a segment number
	 */
	switch (h->reducealg)
	{
		case REDUCE_BITMASK:
			result = FASTMOD(h->hash, (uint32) h->numsegs); /* fast mod (bitmask) */
			break;

		case REDUCE_LAZYMOD:
			result = (h->hash) % (h->numsegs);	/* simple mod */
			break;

		case REDUCE_JUMP_HASH:
			result = jump_consistent_hash(h->hash, h->numsegs);
			break;
	}

	return result;
}

Oid
cdb_default_distribution_opclass_for_type(Oid typeoid)
{
	Oid			opclass;
	Oid			opcintype;

	/* If the datatype has a default hash opclass, use that. */
	opclass = GetDefaultOpClass(typeoid, HASH_AM_OID);
	if (!OidIsValid(opclass))
		return InvalidOid;

	/* extra checks for generic array and range opclasses */
	opcintype = get_opclass_input_type(opclass);
	if (opcintype == ANYARRAYOID && typeoid != ANYARRAYOID)
	{
		Oid			element_type;
		TypeCacheEntry *typentry;

		element_type = get_base_element_type(typeoid);

		/* same check that hash_array() uses */
		typentry = lookup_type_cache(element_type,
									 TYPECACHE_HASH_PROC_FINFO);
		if (!OidIsValid(typentry->hash_proc_finfo.fn_oid))
			return InvalidOid;
	}

	return opclass;
}

Oid
cdb_hashproc_in_opfamily(Oid opfamily, Oid typeoid, bool missing_ok)
{
	Oid			hashfunc;
	CatCList   *catlist;
	int			i;

	/* First try a simple lookup. */
	hashfunc = get_opfamily_proc(opfamily, typeoid, typeoid, HASHPROC);
	if (hashfunc)
		return hashfunc;

	/*
	 * Not found. Check for the case that there is a function for another datatype
	 * that's nevertheless binary coercible. (At least 'varchar' ops rely on this,
	 * to leverage the text operator.
	 */
	catlist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamily));

	for (i = 0; i < catlist->n_members; i++)
	{
		HeapTuple	tuple = &catlist->members[i]->tuple;
		Form_pg_amproc amproc_form = (Form_pg_amproc) GETSTRUCT(tuple);

		if (amproc_form->amprocnum != HASHPROC)
			continue;

		if (amproc_form->amproclefttype != amproc_form->amprocrighttype)
			continue;

		if (IsBinaryCoercible(typeoid, amproc_form->amproclefttype))
		{
			/* found it! */
			hashfunc = amproc_form->amproc;
			break;
		}
	}

	ReleaseSysCacheList(catlist);

	if (!hashfunc && !missing_ok)
		elog(ERROR, "could not find hash function for type %u in operator family %u",
			 typeoid, opfamily);

	return hashfunc;
}

/*================================================================
 *
 * GENERAL PURPOSE UTILS
 *
 *================================================================
 */

/*
 * returns 1 is the input int is a power of 2 and 0 otherwise.
 */
static int
ispowof2(int numsegs)
{
	return !(numsegs & (numsegs - 1));
}

/*
 * The following jump consistent hash algorithm is
 * just the one from the original paper:
 * https://arxiv.org/abs/1406.2294
 */

static inline int32
jump_consistent_hash(uint64 key, int32 num_segments)
{
	int64 b = -1;
	int64 j = 0;
	while (j < num_segments)
	{
		b = j;
		key = key * 2862933555777941757ULL + 1;
		j = (b + 1) * ((double)(1LL << 31) / (double)((key >> 33) + 1));
	}
	return b;
}
