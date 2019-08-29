//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CMemoryContextPool.cpp
//
//	@doc:
//		CMemoryPoolTracker implementation that uses PostgreSQL memory
//		contexts.
//
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"

#include "utils/memutils.h"
}

#include "gpos/memory/CMemoryPool.h"

#include "gpopt/utils/CMemoryContextPool.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMemoryContextPool::CMemoryContextPool
//
//	@doc:
//		Ctor.
//
//---------------------------------------------------------------------------
CMemoryContextPool::CMemoryContextPool()
{
	m_cxt = AllocSetContextCreate(TopMemoryContext,
				      "GPORCA memory pool",
				      ALLOCSET_DEFAULT_MINSIZE,
				      ALLOCSET_DEFAULT_INITSIZE,
				      ALLOCSET_DEFAULT_MAXSIZE);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryContextPool::~CMemoryContextPool
//
//	@doc:
//		Dtor.
//
//---------------------------------------------------------------------------
CMemoryContextPool::~CMemoryContextPool()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::Allocate
//
//	@doc:
//		Allocate memory.
//
//---------------------------------------------------------------------------
void *
CMemoryContextPool::Allocate
	(
	const ULONG bytes,
	const CHAR *file,
	const ULONG line
	)
{
	return MemoryContextAlloc(m_cxt, bytes);
}

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::Free
//
//	@doc:
//		Free memory.
//
//---------------------------------------------------------------------------
void
CMemoryContextPool::Free
	(
	void *ptr
	)
{
	pfree(ptr);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::TearDown
//
//	@doc:
//		Prepare the memory pool to be deleted;
// 		this function is called only once so locking is not required;
//
//---------------------------------------------------------------------------
void
CMemoryContextPool::TearDown()
{
	MemoryContextDelete(m_cxt);
}

// EOF
