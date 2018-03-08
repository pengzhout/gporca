//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		CLeftAntiSemiJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for left anti-semi join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLeftAntiSemiJoinStatsProcessor_H
#define GPNAUCRATES_CLeftAntiSemiJoinStatsProcessor_H

#include "gpos/base.h"
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
	//		CLeftAntiSemiJoinStatsProcessor
	//
	//	@doc:
	//		Processor for computing statistics for left anti-semi join
	//
	//---------------------------------------------------------------------------
	class CLeftAntiSemiJoinStatsProcessor
	{
	private:

	public:
		// helper for LAS-joining histograms
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
						BOOL fEmptyInput, // if true, one of the inputs is empty
						BOOL fIgnoreLasjHistComputation
				);
	};
}

#endif // !GPNAUCRATES_CLeftAntiSemiJoinStatsProcessor_H

// EOF

