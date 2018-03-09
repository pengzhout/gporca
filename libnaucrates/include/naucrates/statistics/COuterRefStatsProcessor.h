//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		COuterRefStatsProcessor.h
//
//	@doc:
//		Compute statistics for all joins
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_COuterRefStatsProcessor_H
#define GPNAUCRATES_COuterRefStatsProcessor_H

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
	//		COuterRefStatsProcessor
	//
	//	@doc:
	//		Parent class for computing statistics for all joins
	//
	//---------------------------------------------------------------------------
	class COuterRefStatsProcessor
	{
	private:


	public:

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
	};

}

#endif // !GPNAUCRATES_COuterRefStatsProcessor_H

// EOF

