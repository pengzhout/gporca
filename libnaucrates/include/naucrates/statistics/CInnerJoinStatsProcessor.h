//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		CInnerJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Inner Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CInnerJoinStatsProcessor_H
#define GPNAUCRATES_CInnerJoinStatsProcessor_H

#include "gpos/base.h"
#include "naucrates/statistics/CStatsPredJoin.h"
#include "naucrates/statistics/IStatistics.h"
#include "gpopt/operators/CExpression.h"
#include "naucrates/statistics/CJoinStatsProcessor.h"


namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpopt;

	//---------------------------------------------------------------------------
	//	@class:
	//		CInnerJoinStatsProcessor
	//
	//	@doc:
	//		Processor for computing statistics for Inner Join
	//
	//---------------------------------------------------------------------------
	class CInnerJoinStatsProcessor : public CJoinStatsProcessor
	{
	private:

	public:

		// inner join with another stats structure
		static
		CStatistics *PstatsInnerJoinStatic(IMemoryPool *pmp, const IStatistics *pistatsOuter, const IStatistics *pistatsInner, DrgPstatspredjoin *pdrgpstatspredjoin);
	};
}

#endif // !GPNAUCRATES_CInnerJoinStatsProcessor_H

// EOF

