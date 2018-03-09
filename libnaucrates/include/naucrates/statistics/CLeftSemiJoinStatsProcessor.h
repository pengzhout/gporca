//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		CLeftSemiJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Left Semi Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLeftSemiJoinStatsProcessor_H
#define GPNAUCRATES_CLeftSemiJoinStatsProcessor_H

#include "gpos/base.h"
#include "naucrates/statistics/CJoinStatsProcessor.h"
#include "naucrates/statistics/CStatsPredJoin.h"
#include "naucrates/statistics/IStatistics.h"
#include "gpopt/operators/CExpression.h"


namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpopt;

	//---------------------------------------------------------------------------
	//	@class:
	//		CLeftSemiJoinStatsProcessor
	//
	//	@doc:
	//		Processor for computing statistics for Left Semi Join
	//
	//---------------------------------------------------------------------------
	class CLeftSemiJoinStatsProcessor : public CJoinStatsProcessor
	{
	private:


	public:

		static
		CStatistics *PstatsLSJoinStatic
				(
						IMemoryPool *pmp,
						const IStatistics *pstatsOuter,
						const IStatistics *pstatsInner,
						DrgPstatspredjoin *pdrgpstatspredjoin
				);
	};
}

#endif // !GPNAUCRATES_CLeftSemiJoinStatsProcessor_H

// EOF

