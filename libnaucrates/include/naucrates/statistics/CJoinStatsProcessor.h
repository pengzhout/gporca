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

	protected:

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


	public:
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


		static
		IStatistics *PstatsJoinArray(IMemoryPool *pmp, BOOL fOuterJoin, DrgPstat *pdrgpstat, CExpression *pexprScalar);

		// derive statistics when scalar expression has outer references
		static
		IStatistics *PstatsDeriveWithOuterRefs
				(
						IMemoryPool *pmp,
						BOOL fOuterJoin, // use outer join semantics for statistics derivation
						CExpressionHandle &exprhdl, // handle attached to the logical expression we want to derive stats for
						CExpression *pexprScalar, // scalar condition used for stats derivation
						IStatistics *pstats, // statistics object of attached expression
						DrgPstat *pdrgpstatOuter // array of stats objects where outer references are defined
				);

		// helper for deriving statistics for join operation based on given scalar expression
		static
		IStatistics *PstatsJoinWithOuterRefs
				(
						IMemoryPool *pmp,
						CExpressionHandle &exprhdl,
						DrgPstat *pdrgpstatChildren,
						CExpression *pexprScalarLocal, // filter expression on local columns only
						CExpression *pexprScalarOuterRefs, // filter expression involving outer references
						DrgPstat *pdrgpstatOuter
				);

		// derive statistics for join operation given array of statistics object
		static
		IStatistics *PstatsJoin(IMemoryPool *pmp, CExpressionHandle &exprhdl, DrgPstat *pdrgpstatCtxt);
	};
}

#endif // !GPNAUCRATES_CJoinStatsProcessor_H

// EOF

