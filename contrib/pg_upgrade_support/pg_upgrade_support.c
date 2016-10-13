/*
 *	pg_upgrade_sysoids.c
 *
 *	server-side functions to set backend global variables
 *	to control oid and relfilenode assignment
 *
 *	Copyright (c) 2010, PostgreSQL Global Development Group
 *	$PostgreSQL: pgsql/contrib/pg_upgrade_support/pg_upgrade_support.c,v 1.5 2010/07/06 19:18:55 momjian Exp $
 */

#include "postgres.h"

#include "fmgr.h"
#include "catalog/dependency.h"
#include "catalog/pg_class.h"
#include "catalog/heap.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern PGDLLIMPORT List *binary_upgrade_next_pg_type_oid;
extern PGDLLIMPORT Oid binary_upgrade_next_array_pg_type_oid;
extern PGDLLIMPORT List *binary_upgrade_next_toast_pg_type_oid;
extern PGDLLIMPORT List *binary_upgrade_next_aosegments_pg_type_oid;
extern PGDLLIMPORT Oid binary_upgrade_next_aoblockdir_pg_type_oid;
extern PGDLLIMPORT List * binary_upgrade_next_aovisimap_pg_type_oid;

extern PGDLLIMPORT List *binary_upgrade_next_heap_pg_class_oid;
extern PGDLLIMPORT List *binary_upgrade_next_index_pg_class_oid;
extern PGDLLIMPORT List *binary_upgrade_next_toast_pg_class_oid;
extern PGDLLIMPORT List *binary_upgrade_next_toast_index_pg_class_oid;
extern PGDLLIMPORT List *binary_upgrade_next_aosegments_pg_class_oid;
extern PGDLLIMPORT List *binary_upgrade_next_aosegments_index_pg_class_oid;
extern PGDLLIMPORT List *binary_upgrade_next_aoblockdir_pg_class_oid;
extern PGDLLIMPORT List *binary_upgrade_next_aoblockdir_index_pg_class_oid;
extern PGDLLIMPORT List *binary_upgrade_next_aovisimap_pg_class_oid;
extern PGDLLIMPORT List *binary_upgrade_next_aovisimap_index_pg_class_oid;

extern PGDLLIMPORT Oid binary_upgrade_next_pg_authid_oid;

Datum		set_next_pg_type_oid(PG_FUNCTION_ARGS);
Datum		set_next_array_pg_type_oid(PG_FUNCTION_ARGS);
Datum		set_next_toast_pg_type_oid(PG_FUNCTION_ARGS);
Datum		set_next_aosegments_pg_type_oid(PG_FUNCTION_ARGS);
Datum		set_next_aoblockdir_pg_type_oid(PG_FUNCTION_ARGS);
Datum		set_next_aovisimap_pg_type_oid(PG_FUNCTION_ARGS);

Datum		clear_next_heap_pg_class_oid(PG_FUNCTION_ARGS);
Datum		set_next_heap_pg_class_oid(PG_FUNCTION_ARGS);
Datum		set_next_index_pg_class_oid(PG_FUNCTION_ARGS);
Datum		set_next_toast_pg_class_oid(PG_FUNCTION_ARGS);
Datum		set_next_toast_index_pg_class_oid(PG_FUNCTION_ARGS);
Datum		set_next_aosegments_pg_class_oid(PG_FUNCTION_ARGS);
Datum		set_next_aosegments_index_pg_class_oid(PG_FUNCTION_ARGS);
Datum		set_next_aoblockdir_pg_class_oid(PG_FUNCTION_ARGS);
Datum		set_next_aoblockdir_index_pg_class_oid(PG_FUNCTION_ARGS);
Datum		set_next_aovisimap_pg_class_oid(PG_FUNCTION_ARGS);
Datum		set_next_aovisimap_index_pg_class_oid(PG_FUNCTION_ARGS);

Datum		set_next_pg_authid_oid(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(set_next_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_array_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_toast_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_aosegments_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_aoblockdir_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_aovisimap_pg_type_oid);

PG_FUNCTION_INFO_V1(clear_next_heap_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_heap_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_index_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_toast_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_toast_index_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_aosegments_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_aosegments_index_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_aoblockdir_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_aoblockdir_index_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_aovisimap_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_aovisimap_index_pg_class_oid);

PG_FUNCTION_INFO_V1(set_next_pg_authid_oid);

static List * appendNameOid(List *dest, char *relname, Oid oid);
static List *appendOidOid(List *dest, Oid targetOid, Oid oid);
static List *freeList(List *list);


Datum
set_next_pg_type_oid(PG_FUNCTION_ARGS)
{
	char		*typname = PG_GETARG_CSTRING(0);
	Oid			typoid = PG_GETARG_OID(1);

	binary_upgrade_next_pg_type_oid =
			appendNameOid(binary_upgrade_next_pg_type_oid, typname, typoid);

	PG_RETURN_VOID();
}

Datum
set_next_array_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);

	binary_upgrade_next_array_pg_type_oid = typoid;

	PG_RETURN_VOID();
}

Datum
set_next_toast_pg_type_oid(PG_FUNCTION_ARGS)
{
	char	*relName = PG_GETARG_CSTRING(0);
	Oid		reloid = PG_GETARG_OID(1);

	binary_upgrade_next_toast_pg_type_oid =
		appendNameOid(binary_upgrade_next_toast_pg_type_oid, relName, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_aosegments_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			targetOid 	= PG_GETARG_OID(0);
	Oid			typoid = PG_GETARG_OID(1);

	binary_upgrade_next_aosegments_pg_type_oid = 
		appendOidOid(binary_upgrade_next_aosegments_pg_type_oid, targetOid, typoid);

	PG_RETURN_VOID();
}

Datum
set_next_aoblockdir_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);

	binary_upgrade_next_aoblockdir_pg_type_oid = typoid;

	PG_RETURN_VOID();
}

Datum
set_next_aovisimap_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			targetOid 	= PG_GETARG_OID(0);
	Oid			typoid 		= PG_GETARG_OID(1);

	binary_upgrade_next_aovisimap_pg_type_oid = 
		appendOidOid(binary_upgrade_next_aovisimap_pg_type_oid,targetOid, typoid);

	PG_RETURN_VOID();
}
/*
 * this is used to ensure the last command's data is cleared out
 */
Datum
clear_next_heap_pg_class_oid(PG_FUNCTION_ARGS)
{
	MemoryContextSwitchTo(TopMemoryContext);

	freeList(binary_upgrade_next_heap_pg_class_oid);
	binary_upgrade_next_heap_pg_class_oid = NIL;

	freeList(binary_upgrade_next_index_pg_class_oid);
	binary_upgrade_next_index_pg_class_oid = NIL;

	freeList(binary_upgrade_next_toast_pg_class_oid);
	binary_upgrade_next_toast_pg_class_oid = NIL;

	freeList(binary_upgrade_next_toast_index_pg_class_oid);
	binary_upgrade_next_toast_index_pg_class_oid = NIL;

	freeList(binary_upgrade_next_aosegments_pg_class_oid);
	binary_upgrade_next_aosegments_pg_class_oid = NIL;

	freeList(binary_upgrade_next_aosegments_index_pg_class_oid);
	binary_upgrade_next_aosegments_index_pg_class_oid = NIL;

	freeList(binary_upgrade_next_aoblockdir_pg_class_oid);
	binary_upgrade_next_aoblockdir_pg_class_oid = NIL;

	freeList(binary_upgrade_next_aoblockdir_index_pg_class_oid);
	binary_upgrade_next_aoblockdir_index_pg_class_oid = NIL;

	freeList(binary_upgrade_next_aovisimap_pg_class_oid);
	binary_upgrade_next_aovisimap_pg_class_oid = NIL;

	freeList(binary_upgrade_next_aovisimap_index_pg_class_oid);
	binary_upgrade_next_aovisimap_index_pg_class_oid = NIL;

	PG_RETURN_VOID();
}
Datum
set_next_heap_pg_class_oid(PG_FUNCTION_ARGS)
{
	char	*tablename = PG_GETARG_CSTRING(0);
	Oid		reloid = PG_GETARG_OID(1);

	binary_upgrade_next_heap_pg_class_oid = 
		appendNameOid(binary_upgrade_next_heap_pg_class_oid, tablename, reloid);

	PG_RETURN_VOID();
}


Datum
set_next_index_pg_class_oid(PG_FUNCTION_ARGS)
{
	char	*tablename = PG_GETARG_CSTRING(0);
	Oid		reloid = PG_GETARG_OID(1);

	binary_upgrade_next_index_pg_class_oid = 
		appendNameOid(binary_upgrade_next_index_pg_class_oid, tablename, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_toast_pg_class_oid(PG_FUNCTION_ARGS)
{
        Oid	targetOid = PG_GETARG_OID(0);	
	Oid	reloid = PG_GETARG_OID(1);

	binary_upgrade_next_toast_pg_class_oid = 
		appendOidOid(binary_upgrade_next_toast_pg_class_oid, targetOid, reloid);


	PG_RETURN_VOID();
}

/*
 * In PostgreSQL, we use set_next_index_pg_class_oid() to set the
 * index OID for a toast index. In GPDB, we couldn't do that for
 * aosegments, aoblockidr and aovisimap, because you could have all
 * of those in addition to the toast table. For clarity, we have
 * a separate function for the toast index in GPDB as well.
 */
Datum
set_next_toast_index_pg_class_oid(PG_FUNCTION_ARGS)
{
        Oid	targetOid = PG_GETARG_OID(0);	
	Oid	reloid = PG_GETARG_OID(1);

	binary_upgrade_next_toast_index_pg_class_oid = 
		appendOidOid(binary_upgrade_next_toast_index_pg_class_oid, targetOid, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_aosegments_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid		target_oid = PG_GETARG_OID(0);
	Oid		reloid = PG_GETARG_OID(1);

	binary_upgrade_next_aosegments_pg_class_oid = 
		appendOidOid(binary_upgrade_next_aosegments_pg_class_oid, target_oid, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_aosegments_index_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid		target_oid = PG_GETARG_OID(0);
	Oid		reloid = PG_GETARG_OID(1);

	binary_upgrade_next_aosegments_index_pg_class_oid = 
		appendOidOid(binary_upgrade_next_aosegments_index_pg_class_oid, target_oid, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_aoblockdir_pg_class_oid(PG_FUNCTION_ARGS)
{
	char	*tablename = PG_GETARG_CSTRING(0);
	Oid		reloid = PG_GETARG_OID(1);

	binary_upgrade_next_aoblockdir_pg_class_oid = 
		appendNameOid(binary_upgrade_next_aoblockdir_pg_class_oid, tablename, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_aoblockdir_index_pg_class_oid(PG_FUNCTION_ARGS)
{
	char	*tablename = PG_GETARG_CSTRING(0);
	Oid		reloid = PG_GETARG_OID(1);

	binary_upgrade_next_aoblockdir_index_pg_class_oid = 
		appendNameOid(binary_upgrade_next_aoblockdir_index_pg_class_oid, tablename, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_aovisimap_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid		targetOid 	= PG_GETARG_OID(0);
	Oid		reloid		= PG_GETARG_OID(1);

	binary_upgrade_next_aovisimap_pg_class_oid = 
		appendOidOid(binary_upgrade_next_aovisimap_pg_class_oid, targetOid, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_aovisimap_index_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid		targetOid 	= PG_GETARG_OID(0);
	Oid		reloid 		= PG_GETARG_OID(1);

	binary_upgrade_next_aovisimap_index_pg_class_oid =
		appendOidOid(binary_upgrade_next_aovisimap_index_pg_class_oid, targetOid, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_pg_authid_oid(PG_FUNCTION_ARGS)
{
	Oid			authoid = PG_GETARG_OID(0);

	binary_upgrade_next_pg_authid_oid = authoid;
	PG_RETURN_VOID();
}

static List *
appendNameOid(List *dest, char *relname, Oid oid)
{
	MemoryContextSwitchTo(TopMemoryContext);

	RelationNameOid *name_oid = palloc(sizeof(RelationNameOid));
	strncpy(name_oid->tablename,relname,NAMEDATALEN);
	name_oid->reloid = oid;

	return lappend(dest, name_oid);
}
static List *
appendOidOid(List *dest, Oid targetOid, Oid oid)
{
	MemoryContextSwitchTo(TopMemoryContext);

	RelationOidOid *oid_oid = palloc(sizeof(RelationOidOid));
	oid_oid->targetOid = targetOid;
	oid_oid->reloid = oid;

	return lappend(dest, oid_oid);
}
static List *
freeList(List *list)
{

	while (list != NIL && list->length > 0)
	{
		list = list_delete_first(list);
	}
	return list;
}
