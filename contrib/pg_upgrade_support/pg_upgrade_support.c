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
#include "utils/builtins.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern PGDLLIMPORT HTAB	*relation_oid_hash;

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

static HTAB *createOidHtab();
static void insertRelnameOid(char *relname, Oid reloid);



Datum
set_next_pg_type_oid(PG_FUNCTION_ARGS)
{
	char		*typName = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid			typOid = PG_GETARG_OID(1);

	insertRelnameOid(typName, typOid);

	PG_RETURN_VOID();
}

Datum
set_next_array_pg_type_oid(PG_FUNCTION_ARGS)
{
	char	*typName = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		typoid = PG_GETARG_OID(1);

	insertRelnameOid(typName, typoid);


	PG_RETURN_VOID();
}

Datum
set_next_toast_pg_type_oid(PG_FUNCTION_ARGS)
{
	char	*relName = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		typoid = PG_GETARG_OID(1);

	insertRelnameOid(relName, typoid);

	PG_RETURN_VOID();
}

Datum
set_next_aosegments_pg_type_oid(PG_FUNCTION_ARGS)
{
	char	*relName = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		typoid = PG_GETARG_OID(1);

	insertRelnameOid(relName, typoid);

	PG_RETURN_VOID();
}

Datum
set_next_aoblockdir_pg_type_oid(PG_FUNCTION_ARGS)
{
	char	*relName = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		typoid = PG_GETARG_OID(1);

	insertRelnameOid(relName, typoid);

	PG_RETURN_VOID();
}

Datum
set_next_aovisimap_pg_type_oid(PG_FUNCTION_ARGS)
{
	char		*typName 	= text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid			typOid 		= PG_GETARG_OID(1);

	insertRelnameOid(typName, typOid);

	PG_RETURN_VOID();
}
/*
 * this is used to ensure the last command's data is cleared out
 */
Datum
clear_next_heap_pg_class_oid(PG_FUNCTION_ARGS)
{
	MemoryContextSwitchTo(TopMemoryContext);

	PG_RETURN_VOID();
}
Datum
set_next_heap_pg_class_oid(PG_FUNCTION_ARGS)
{
	char	*tablename = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		reloid = PG_GETARG_OID(1);

	insertRelnameOid(tablename, reloid);

	PG_RETURN_VOID();
}



Datum
set_next_index_pg_class_oid(PG_FUNCTION_ARGS)
{
	char	*tablename = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		reloid = PG_GETARG_OID(1);

	insertRelnameOid(tablename, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_toast_pg_class_oid(PG_FUNCTION_ARGS)
{
    char *relname = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid	reloid = PG_GETARG_OID(1);

	insertRelnameOid(relname, reloid);

	PG_RETURN_VOID();
}

/*
 * In PostgreSQL, we use set_next_index_pg_class_oid() to set the
 * index OID for a toast index. In GPDB, we couldn't do that for
 * aosegments, aoblockidr and aovisimap, because you could have all
 * of those in addition to the toast table. For clarity, we have
 * a separate function for the toast index in GPDB as well.
 */
/* TODO CHANGE FUNCTION CALL */
Datum
set_next_toast_index_pg_class_oid(PG_FUNCTION_ARGS)
{
    char *relname = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid	reloid = PG_GETARG_OID(1);

	char fullyQualifiedName[NAMEDATALEN*3]="pg_toast.";
	strcpy(fullyQualifiedName,relname);

	insertRelnameOid(fullyQualifiedName, reloid);

	PG_RETURN_VOID();
}
/* TODO CHANGE function call */
Datum
set_next_aosegments_pg_class_oid(PG_FUNCTION_ARGS)
{
    char *relname = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		reloid = PG_GETARG_OID(1);

	char fullyQualifiedName[NAMEDATALEN*3]="pg_aoseg.";
	strcpy(fullyQualifiedName,relname);

	insertRelnameOid(fullyQualifiedName, reloid);

	PG_RETURN_VOID();
}

/* TODO: CHANGE FUNCTION TYPE */
Datum
set_next_aosegments_index_pg_class_oid(PG_FUNCTION_ARGS)
{
    char *relname = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		reloid = PG_GETARG_OID(1);

	char fullyQualifiedName[NAMEDATALEN*3]="pg_aoseg.";
	strcpy(fullyQualifiedName,relname);

	insertRelnameOid(fullyQualifiedName, reloid);

	PG_RETURN_VOID();
}

/* TODO: CHANGE FUNC DEF */
Datum
set_next_aoblockdir_pg_class_oid(PG_FUNCTION_ARGS)
{
	char	*relname = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		reloid = PG_GETARG_OID(1);

	insertRelnameOid(relname, reloid);

	PG_RETURN_VOID();
}

/* TODO: CHANGE FUNC DEF */
Datum
set_next_aoblockdir_index_pg_class_oid(PG_FUNCTION_ARGS)
{
	char	*relname = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		reloid = PG_GETARG_OID(1);

	insertRelnameOid(relname, reloid);

	PG_RETURN_VOID();
}

/* TODO: CHANGE FUNC DEF */
Datum
set_next_aovisimap_pg_class_oid(PG_FUNCTION_ARGS)
{
	char	*relname = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		reloid		= PG_GETARG_OID(1);

	insertRelnameOid(relname, reloid);

	PG_RETURN_VOID();
}
/* TODO: CHANGE FUNC DEF */
Datum
set_next_aovisimap_index_pg_class_oid(PG_FUNCTION_ARGS)
{
	char	*relname = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		reloid 		= PG_GETARG_OID(1);

	insertRelnameOid(relname, reloid);

	PG_RETURN_VOID();
}

Datum
set_next_pg_authid_oid(PG_FUNCTION_ARGS)
{
	char	*userName = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid		authoid = PG_GETARG_OID(1);

	insertRelnameOid(userName, authoid);

	PG_RETURN_VOID();
}


static HTAB *
createOidHtab()
{
	HASHCTL		ctl;

	ctl.keysize = NAMEDATALEN*3;
	ctl.entrysize = sizeof(relname_oid_hash_entry);

	return hash_create("Relation Oid Hash", 32, &ctl, HASH_ELEM);
}

static void
insertRelnameOid(char *relname, Oid reloid)
{
	char    *key;

	MemoryContextSwitchTo(TopMemoryContext);

	key=pstrdup(relname);

	if (relation_oid_hash == NULL)
	{
		relation_oid_hash = createOidHtab();
	}
	Assert(	relation_oid_hash );

	relname_oid_hash_entry *oid_entry = hash_search(relation_oid_hash, key, HASH_ENTER, NULL);

	Assert( oid_entry );

	oid_entry->reloid = reloid;
}
