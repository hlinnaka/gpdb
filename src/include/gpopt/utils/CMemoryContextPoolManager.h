//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CMemoryContextPoolManager.h
//
//	@doc:
//		Bridge between PostgreSQL memory contexts and GPORCA memory pools.
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CMemoryContextPoolManager_H
#define GPDXL_CMemoryContextPoolManager_H

#include "gpos/base.h"

#include "gpos/memory/CMemoryPoolManager.h"

namespace gpos
{
	// memory pool manager that uses GPDB memory contexts
	class CMemoryContextPoolManager : public CMemoryPoolManager
	{
		protected:

			// dtor
			virtual
			~CMemoryContextPoolManager();

			// private no copy ctor
			CMemoryContextPoolManager(const CMemoryContextPoolManager&);

		public:

			// ctor
			CMemoryContextPoolManager();

			// create new memory pool
			CMemoryPool *Create
				(
				CMemoryPoolManager::AllocType alloc_type
				);

			// release memory pool
			void Destroy(CMemoryPool *);

			// delete memory pools and release manager
			void Shutdown()
			{
			}

#ifdef GPOS_DEBUG

			// check if the memory pool keeps track of live objects
			BOOL SupportsLiveObjectWalk() const
			{
				return false;
			}

			// walk the live objects
			void WalkLiveObjects(gpos::IMemoryVisitor *visitor) const
			{
			}

			// check if statistics tracking is supported
			BOOL SupportsStatistics() const
			{
				return true;
			}

			// return the current statistics
			void UpdateStatistics(CMemoryPoolStatistics &memory_pool_statistics)
			{
			}

#endif // GPOS_DEBUG

	};
}

#endif // !GPDXL_CMemoryContextPool_H

// EOF
