//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2013 EMC Corp.
//
//	@filename:
//		CInnerJoinStatsProcessor.cpp
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
#include "naucrates/statistics/CInnerJoinStatsProcessor.h"
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


//	derive statistics for the given join predicate
IStatistics *
CInnerJoinStatsProcessor::PstatsJoinArray
		(
				IMemoryPool *pmp,
				BOOL fOuterJoin,
				DrgPstat *pdrgpstat,
				CExpression *pexprScalar
		)
{
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != pdrgpstat);
	GPOS_ASSERT(0 < pdrgpstat->UlLength());
	GPOS_ASSERT_IMP(fOuterJoin, 2 == pdrgpstat->UlLength());

	// create an empty set of outer references for statistics derivation
	CColRefSet *pcrsOuterRefs = GPOS_NEW(pmp) CColRefSet(pmp);

	// join statistics objects one by one using relevant predicates in given scalar expression
	const ULONG ulStats = pdrgpstat->UlLength();
	IStatistics *pstats = (*pdrgpstat)[0]->PstatsCopy(pmp);
	CDouble dRowsOuter = pstats->DRows();

	for (ULONG ul = 1; ul < ulStats; ul++)
	{
		IStatistics *pstatsCurrent = (*pdrgpstat)[ul];

		DrgPcrs *pdrgpcrsOutput= GPOS_NEW(pmp) DrgPcrs(pmp);
		pdrgpcrsOutput->Append(pstats->Pcrs(pmp));
		pdrgpcrsOutput->Append(pstatsCurrent->Pcrs(pmp));

		CStatsPred *pstatspredUnsupported = NULL;
		DrgPstatspredjoin *pdrgpstatspredjoin = CStatsPredUtils::PdrgpstatspredjoinExtract
				(
						pmp,
						pexprScalar,
						pdrgpcrsOutput,
						pcrsOuterRefs,
						&pstatspredUnsupported
				);
		IStatistics *pstatsNew = NULL;
		if (fOuterJoin)
		{
			pstatsNew = pstats->PstatsLOJ(pmp, pstatsCurrent, pdrgpstatspredjoin);
		}
		else
		{

			pstatsNew = pstats->PstatsInnerJoin(pmp, pstatsCurrent, pdrgpstatspredjoin);
		}
		pstats->Release();
		pstats = pstatsNew;

		if (NULL != pstatspredUnsupported)
		{
			// apply the unsupported join filters as a filter on top of the join results.
			// TODO,  June 13 2014 we currently only cap NDVs for filters
			// immediately on top of tables.
			IStatistics *pstatsAfterJoinFilter = pstats->PstatsFilter
					(
							pmp,
							pstatspredUnsupported,
							false /* fCapNdvs */
					);

			// If it is outer join and the cardinality after applying the unsupported join
			// filters is less than the cardinality of outer child, we don't use this stats.
			// Because we need to make sure that Card(LOJ) >= Card(Outer child of LOJ).
			if (fOuterJoin && pstatsAfterJoinFilter->DRows() < dRowsOuter)
			{
				pstatsAfterJoinFilter->Release();
			}
			else
			{
				pstats->Release();
				pstats = pstatsAfterJoinFilter;
			}

			pstatspredUnsupported->Release();
		}

		pdrgpstatspredjoin->Release();
		pdrgpcrsOutput->Release();
	}

	// clean up
	pcrsOuterRefs->Release();

	return pstats;
}


// helper for inner-joining histograms
// TODO: rename this function because it is used for all joins except left anti-semi join
void
CInnerJoinStatsProcessor::JoinHistograms
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
				BOOL fEmptyInput
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
		// use Cartesian product as scale factor
		*pdScaleFactor = dRows1 * dRows2;
		*pphist1 =  GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp));
		*pphist2 =  GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp));

		return;
	}

	*pdScaleFactor = CScaleFactorUtils::DDefaultScaleFactorJoin;

	BOOL fEmptyHistograms = phist1->FEmpty() || phist2->FEmpty();

	if (fEmptyHistograms)
	{
		// if one more input has no histograms (due to lack of statistics
		// for table columns or computed columns), we estimate
		// the join cardinality to be the max of the two rows.
		// In other words, the scale factor is equivalent to the
		// min of the two rows.
		*pdScaleFactor = std::min(dRows1, dRows2);
	}
	else if (CHistogram::FSupportsJoin(escmpt))
	{
		CHistogram *phistJoin = phist1->PhistJoinNormalized
				(
						pmp,
						escmpt,
						dRows1,
						phist2,
						dRows2,
						pdScaleFactor
				);

		if (CStatsPred::EstatscmptEq == escmpt || CStatsPred::EstatscmptINDF == escmpt)
		{
			if (phist1->FScaledNDV())
			{
				phistJoin->SetNDVScaled();
			}
			*pphist1 = phistJoin;
			*pphist2 = (*pphist1)->PhistCopy(pmp);
			if (phist2->FScaledNDV())
			{
				(*pphist2)->SetNDVScaled();
			}
			return;
		}

		// note that IDF and Not Equality predicate we do not generate histograms but
		// just the scale factors.

		GPOS_ASSERT(phistJoin->FEmpty());
		GPOS_DELETE(phistJoin);

		// TODO:  Feb 21 2014, for all join condition except for "=" join predicate
		// we currently do not compute new histograms for the join columns
	}

	// not supported join operator or missing histograms,
	// copy input histograms and use default scale factor
	*pphist1 = phist1->PhistCopy(pmp);
	*pphist2 = phist2->PhistCopy(pmp);
}

// EOF
