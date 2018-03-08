//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		CJoinStatsProcessor.h
//
//	@doc:
//		Compute statistics for all joins
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CJoinStatsProcessor_H
#define GPNAUCRATES_CJoinStatsProcessor_H

#include "gpos/base.h"
#include "naucrates/statistics/CStatsPredJoin.h"
#include "naucrates/statistics/IStatistics.h"
#include "naucrates/statistics/CStatistics.h"
#include "gpopt/operators/CExpression.h"


namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpopt;

	//---------------------------------------------------------------------------
	//	@class:
	//		CJoinStatsProcessor
	//
	//	@doc:
	//		Parent class for computing statistics for all joins
	//
	//---------------------------------------------------------------------------
	class CJoinStatsProcessor
	{
	private:


		// return join cardinality based on scaling factor and join type
		static
		CDouble DJoinCardinality
				(
						CStatisticsConfig *pstatsconf,
						CDouble dRowsLeft,
						CDouble dRowsRight,
						DrgPdouble *pdrgpd,
						IStatistics::EStatsJoinType esjt
				);

		// for the output stats object, compute its upper bound cardinality mapping based on the bounding method
		// estimated output cardinality and information maintained in the current stats object
		static
		void ComputeCardUpperBounds
				(
						IMemoryPool *pmp, // memory pool
						const CStatistics *pstatsInput,
						CStatistics *pstatsOutput, // output statistics object that is to be updated
						CDouble dRowsOutput, // estimated output cardinality of the operator
						CStatistics::ECardBoundingMethod ecbm // technique used to estimate max source cardinality in the output stats object
				);

		//	check if the join statistics object is empty output based on the input
		//	histograms and the join histograms
		static
		BOOL FEmptyJoinStats
				(
						BOOL fEmptyOuter,
						BOOL fEmptyOutput,
						BOOL fLASJ,
						const CHistogram *phistOuter,
						const CHistogram *phistInner,
						CHistogram *phistJoin
				);

		// create a new hash map of histograms from the results of the inner join and the histograms of the outer child
		static
		HMUlHist *PhmulhistLOJ
				(
						IMemoryPool *pmp,
						const CStatistics *pstatsOuter,
						const CStatistics *pstatsInner,
						CStatistics *pstatsInnerJoin,
						DrgPstatspredjoin *pdrgpstatspredjoin,
						CDouble dRowsInnerJoin,
						CDouble *pdRowsLASJ
				);
		// helper method to add histograms of the inner side of a LOJ
		static
		void AddHistogramsLOJInner
				(
						IMemoryPool *pmp,
						const CStatistics *pstatsInnerJoin,
						DrgPul *pdrgpulInnerColId,
						CDouble dRowsLASJ,
						CDouble dRowsInnerJoin,
						HMUlHist *phmulhistLOJ
				);
	public:

		// main driver to generate join stats
		static
		CStatistics *PstatsJoinDriver
						(
						IMemoryPool *pmp,
						CStatisticsConfig *pstatsconf,
						const IStatistics *pistatsOuter,
						const IStatistics *pistatsInner,
						DrgPstatspredjoin *pdrgpstatspredjoin,
						IStatistics::EStatsJoinType ejst,
						BOOL fIgnoreLasjHistComputation
						);

		// helper for joining histograms
		static
		void JoinHistograms
				(
						IMemoryPool *pmp,
						const CHistogram *phist1,
						const CHistogram *phist2,
						CStatsPredJoin *pstatsjoin,
						CDouble dRows1,
						CDouble dRows2,
						BOOL fLASJ, // if true, use anti-semi join semantics, otherwise use inner join semantics
						CHistogram **pphist1, // output: histogram 1 after join
						CHistogram **pphist2, // output: histogram 2 after join
						CDouble *pdScaleFactor, // output: scale factor based on the join
						BOOL fEmptyInput, // if true, one of the inputs is empty
						BOOL fIgnoreLasjHistComputation
				);
		static
		IStatistics *PstatsJoinArray(IMemoryPool *pmp, BOOL fOuterJoin, DrgPstat *pdrgpstat, CExpression *pexprScalar);

		static
		// semi join stats computation
		CStatistics *PstatsLOJStatic(IMemoryPool *pmp, const IStatistics *pstatsOuter, const IStatistics *pstatsInner, DrgPstatspredjoin *pdrgpstatspredjoin);

		// inner join with another stats structure
		static
		CStatistics *PstatsInnerJoinStatic(IMemoryPool *pmp, const IStatistics *pistatsOuter, const IStatistics *pistatsInner, DrgPstatspredjoin *pdrgpstatspredjoin);
	};
}

#endif // !GPNAUCRATES_CJoinStatsProcessor_H

// EOF

