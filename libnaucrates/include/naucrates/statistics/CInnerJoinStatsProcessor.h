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

		// helper for inner-joining histograms
		static
		void JoinHistograms
				(
						IMemoryPool *pmp,
						const CHistogram *phist1,
						const CHistogram *phist2,
						CStatsPredJoin *pstatsjoin,
						CDouble dRows1,
						CDouble dRows2,
						CHistogram **pphist1, // output: histogram 1 after join
						CHistogram **pphist2, // output: histogram 2 after join
						CDouble *pdScaleFactor, // output: scale factor based on the join
						BOOL fEmptyInput // if true, one of the inputs is empty
				);
		// inner join with another stats structure
		static
		CStatistics *PstatsInnerJoinStatic(IMemoryPool *pmp, const IStatistics *pistatsOuter, const IStatistics *pistatsInner, DrgPstatspredjoin *pdrgpstatspredjoin);
	};
}

#endif // !GPNAUCRATES_CInnerJoinStatsProcessor_H

// EOF

