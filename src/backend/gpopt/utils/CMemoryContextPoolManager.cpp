//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CMemoryContextPoolManager.cpp
//
//	@doc:
//		CMemoryContextPoolManager implementation that uses PostgreSQL
//		memory contexts.
//
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"

#include "utils/memutils.h"
}

#include "gpopt/utils/CMemoryContextPool.h"
#include "gpopt/utils/CMemoryContextPoolManager.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMemoryContextPoolManager::CMemoryContextPoolManager
//
//	@doc:
//		Ctor.
//
//---------------------------------------------------------------------------
CMemoryContextPoolManager::CMemoryContextPoolManager()
{
	m_global_memory_pool = Create(EatTracker);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryContextPoolManager::~CMemoryContextPoolManager
//
//	@doc:
//		Dtor.
//
//---------------------------------------------------------------------------
CMemoryContextPoolManager::~CMemoryContextPoolManager()
{
}

CMemoryPool *
CMemoryContextPoolManager::Create(CMemoryPoolManager::AllocType alloc_type)
{
	/*
	 * We use the same implementation for all "kinds" of pools.
	 * 'alloc_type' is ignored.
	 */
	return new CMemoryContextPool();
}


void
CMemoryContextPoolManager::Destroy(CMemoryPool *mp)
{
	mp->TearDown();
	delete (CMemoryContextPool *) mp;
}

// EOF
