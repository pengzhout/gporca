//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2013 EMC Corp.
//
//	@filename:
//		CLeftAntiSemiJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/exception.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CExpressionUtils.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatsPredUtils.h"
#include "naucrates/statistics/CStatsPredDisj.h"
#include "naucrates/statistics/CStatsPredConj.h"
#include "naucrates/statistics/CStatsPredLike.h"
#include "naucrates/statistics/CScaleFactorUtils.h"
#include "naucrates/statistics/CHistogram.h"

#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeInt2.h"
#include "naucrates/md/IMDTypeInt8.h"
#include "naucrates/md/IMDTypeOid.h"
#include "naucrates/md/CMDIdColStats.h"


using namespace gpopt;
using namespace gpmd;


// helper for LAS-joining histograms
void
CLeftAntiSemiJoinStatsProcessor::JoinHistograms
		(
				IMemoryPool *pmp,
				const CHistogram *phist1,
				const CHistogram *phist2,
				CStatsPredJoin *pstatsjoin,
				CDouble dRows1,
				CDouble ,//dRows2,
				CHistogram **pphist1, // output: histogram 1 after join
				CHistogram **pphist2, // output: histogram 2 after join
				CDouble *pdScaleFactor, // output: scale factor based on the join
				BOOL fEmptyInput,
				BOOL fIgnoreLasjHistComputation
		)
{
	GPOS_ASSERT(NULL != phist1);
	GPOS_ASSERT(NULL != phist2);
	GPOS_ASSERT(NULL != pstatsjoin);
	GPOS_ASSERT(NULL != pphist1);
	GPOS_ASSERT(NULL != pphist2);
	GPOS_ASSERT(NULL != pdScaleFactor);

	*pdScaleFactor = 1.0;

	CStatsPred::EStatsCmpType escmpt = pstatsjoin->Escmpt();

	if (fEmptyInput)
	{
		// anti-semi join should give the full outer side.
		// use 1.0 as scale factor if anti semi join
		*pdScaleFactor = 1.0;
		*pphist1 = phist1->PhistCopy(pmp);
		*pphist2 = NULL;

		return;
	}

	BOOL fEmptyHistograms = phist1->FEmpty() || phist2->FEmpty();
	if (!fEmptyHistograms && CHistogram::FSupportsJoin(escmpt))
	{
		*pphist1 = phist1->PhistLASJoinNormalized
				(
						pmp,
						escmpt,
						dRows1,
						phist2,
						pdScaleFactor,
						fIgnoreLasjHistComputation
				);
		*pphist2 = NULL;

		if ((*pphist1)->FEmpty())
		{
			// if the LASJ histograms is empty then all tuples of the outer join column
			// joined with those on the inner side. That is, LASJ will produce no rows
			*pdScaleFactor = dRows1;
		}

		return;
	}

	// not supported join operator or missing stats,
	// copy input histograms and use default scale factor
	*pdScaleFactor = CDouble(CScaleFactorUtils::DDefaultScaleFactorJoin);
	*pphist1 = phist1->PhistCopy(pmp);
	*pphist2 = NULL;
}


// EOF
