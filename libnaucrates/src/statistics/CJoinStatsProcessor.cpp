//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 EMC Corp.
//
//	@filename:
//		CJoinStatsProcessor.cpp
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
#include "naucrates/statistics/CJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftOuterJoinStatsProcessor.h"
#include "naucrates/statistics/CInnerJoinStatsProcessor.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatsPredJoin.h"
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


// helper for joining histograms
void
CJoinStatsProcessor::JoinHistograms
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

	if (fLASJ)
	{
		CLeftAntiSemiJoinStatsProcessor::JoinHistograms
				(
						pmp,
						phist1,
						phist2,
						pstatsjoin,
						dRows1,
						dRows2,
						pphist1,
						pphist2,
						pdScaleFactor,
						fEmptyInput,
						fIgnoreLasjHistComputation
				);

		return;
	}

	CInnerJoinStatsProcessor::JoinHistograms(pmp, phist1, phist2, pstatsjoin, dRows1, dRows2, pphist1, pphist2, pdScaleFactor, fEmptyInput);
}

//	derive statistics for the given join predicate
IStatistics *
CJoinStatsProcessor::PstatsJoinArray
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


// main driver to generate join stats
CStatistics *
CJoinStatsProcessor::PstatsJoinDriver
		(
				IMemoryPool *pmp,
				CStatisticsConfig *pstatsconf,
				const IStatistics *pistatsOuter,
				const IStatistics *pistatsInner,
				DrgPstatspredjoin *pdrgppredInfo,
				IStatistics::EStatsJoinType esjt,
				BOOL fIgnoreLasjHistComputation
		)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pistatsInner);
	GPOS_ASSERT(NULL != pistatsOuter);

	GPOS_ASSERT(NULL != pdrgppredInfo);

	BOOL fLASJ = (IStatistics::EsjtLeftAntiSemiJoin == esjt);
	BOOL fSemiJoin = IStatistics::FSemiJoin(esjt);

	const CStatistics *pstatsOuter = dynamic_cast<const CStatistics *> (pistatsOuter);
	const CStatistics *pstatsInner = dynamic_cast<const CStatistics *> (pistatsInner);

	// create hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhistJoin = GPOS_NEW(pmp) HMUlHist(pmp);

	// build a bitset with all join columns
	CBitSet *pbsJoinColIds = GPOS_NEW(pmp) CBitSet(pmp);
	for (ULONG ul = 0; ul < pdrgppredInfo->UlLength(); ul++)
	{
		CStatsPredJoin *pstatsjoin = (*pdrgppredInfo)[ul];

		(void) pbsJoinColIds->FExchangeSet(pstatsjoin->UlColId1());
		if (!fSemiJoin)
		{
			(void) pbsJoinColIds->FExchangeSet(pstatsjoin->UlColId2());
		}
	}

	// histograms on columns that do not appear in join condition will
	// be copied over to the result structure
	pstatsOuter->AddNotExcludedHistograms(pmp, pbsJoinColIds, phmulhistJoin);
	if (!fSemiJoin)
	{
		pstatsInner->AddNotExcludedHistograms(pmp, pbsJoinColIds, phmulhistJoin);
	}

	DrgPdouble *pdrgpd = GPOS_NEW(pmp) DrgPdouble(pmp);
	const ULONG ulJoinConds = pdrgppredInfo->UlLength();

	BOOL fEmptyOutput = false;
	CDouble dRowsJoin = 0;
	// iterate over joins
	for (ULONG ul = 0; ul < ulJoinConds; ul++)
	{
		CStatsPredJoin *ppredInfo = (*pdrgppredInfo)[ul];
		ULONG ulColId1 = ppredInfo->UlColId1();
		ULONG ulColId2 = ppredInfo->UlColId2();
		GPOS_ASSERT(ulColId1 != ulColId2);
		// find the histograms corresponding to the two columns
		const CHistogram *phistOuter = pstatsOuter->Phist(ulColId1);
		// are column id1 and 2 always in the order of outer inner?
		const CHistogram *phistInner = pstatsInner->Phist(ulColId2);
		GPOS_ASSERT(NULL != phistOuter);
		GPOS_ASSERT(NULL != phistInner);
		BOOL fEmptyInput = CStatistics::FEmptyJoinInput(pstatsOuter, pstatsInner, fLASJ);

		CDouble dScaleFactorLocal(1.0);
		CHistogram *phistOuterAfter = NULL;
		CHistogram *phistInnerAfter = NULL;
		CJoinStatsProcessor::JoinHistograms
				(
						pmp,
						phistOuter,
						phistInner,
						ppredInfo,
						pstatsOuter->DRows(),
						pstatsInner->DRows(),
						fLASJ,
						&phistOuterAfter,
						&phistInnerAfter,
						&dScaleFactorLocal,
						fEmptyInput,
						fIgnoreLasjHistComputation
				);

		fEmptyOutput = FEmptyJoinStats(pstatsOuter->FEmpty(), fEmptyOutput, fLASJ, phistOuter, phistInner, phistOuterAfter);

		CStatisticsUtils::AddHistogram(pmp, ulColId1, phistOuterAfter, phmulhistJoin);
		if (!fSemiJoin)
		{
			CStatisticsUtils::AddHistogram(pmp, ulColId2, phistInnerAfter, phmulhistJoin);
		}
		GPOS_DELETE(phistOuterAfter);
		GPOS_DELETE(phistInnerAfter);

		pdrgpd->Append(GPOS_NEW(pmp) CDouble(dScaleFactorLocal));
	}


	dRowsJoin = CStatistics::DMinRows;
	if (!fEmptyOutput)
	{
		dRowsJoin = DJoinCardinality(pstatsconf, pstatsOuter->DRows(), pstatsInner->DRows(), pdrgpd, esjt);
	}

	// clean up
	pdrgpd->Release();
	pbsJoinColIds->Release();

	HMUlDouble *phmuldoubleWidthResult = GPOS_NEW(pmp) HMUlDouble(pmp);
	CStatisticsUtils::AddWidthInfo(pmp, pstatsOuter->PHMUlDoubleWidth(), phmuldoubleWidthResult);
	if (!fSemiJoin)
	{
		CStatisticsUtils::AddWidthInfo(pmp, pstatsInner->PHMUlDoubleWidth(), phmuldoubleWidthResult);
	}

	// make a new unsupported join stats class
	// create an output stats object
	CStatistics *pstatsJoin = GPOS_NEW(pmp) CStatistics
			(
					pmp,
					phmulhistJoin,
					phmuldoubleWidthResult,
					dRowsJoin,
					fEmptyOutput,
					pstatsOuter->UlNumberOfPredicates()
			);

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated join cardinality.

	ComputeCardUpperBounds(pmp, pstatsOuter, pstatsJoin, dRowsJoin, CStatistics::EcbmMin /* ecbm */);
	if (!fSemiJoin)
	{
		ComputeCardUpperBounds(pmp, pstatsInner, pstatsJoin, dRowsJoin, CStatistics::EcbmMin /* ecbm */);
	}

		return pstatsJoin;
}


// return join cardinality based on scaling factor and join type
CDouble
CJoinStatsProcessor::DJoinCardinality
		(
				CStatisticsConfig *pstatsconf,
				CDouble dRowsLeft,
				CDouble dRowsRight,
				DrgPdouble *pdrgpd,
				IStatistics::EStatsJoinType esjt
		)
{
	GPOS_ASSERT(NULL != pstatsconf);
	GPOS_ASSERT(NULL != pdrgpd);

	CDouble dScaleFactor = CScaleFactorUtils::DCumulativeJoinScaleFactor(pstatsconf, pdrgpd);
	CDouble dCartesianProduct = dRowsLeft * dRowsRight;

	BOOL fLASJ = IStatistics::EsjtLeftAntiSemiJoin == esjt;
	BOOL fLeftSemiJoin = IStatistics::EsjtLeftSemiJoin == esjt;
	if (fLASJ || fLeftSemiJoin)
	{
		CDouble dRows = dRowsLeft;

		if (fLASJ)
		{
			dRows = dRowsLeft / dScaleFactor;
		}
		else
		{
			// semi join results cannot exceed size of outer side
			dRows = std::min(dRowsLeft.DVal(), (dCartesianProduct / dScaleFactor).DVal());
		}

		return std::max(DOUBLE(1.0), dRows.DVal());
	}

	GPOS_ASSERT(CStatistics::DMinRows <= dScaleFactor);

	return std::max(CStatistics::DMinRows.DVal(), (dCartesianProduct / dScaleFactor).DVal());
}

//	for the output statistics object, compute its upper bound cardinality
// 	mapping based on the bounding method estimated output cardinality
//  and information maintained in the current statistics object
void
CJoinStatsProcessor::ComputeCardUpperBounds
		(
				IMemoryPool *pmp,
				const CStatistics *pstatsInput,
				CStatistics *pstatsOutput, // output statistics object that is to be updated
				CDouble dRowsOutput, // estimated output cardinality of the operator
				CStatistics::ECardBoundingMethod ecbm // technique used to estimate max source cardinality in the output stats object
		)
{
	GPOS_ASSERT(NULL != pstatsOutput);
	GPOS_ASSERT(CStatistics::EcbmSentinel != ecbm);

	const DrgPubndvs *pdrgubndvInput = pstatsInput->Pdrgundv();
	ULONG ulLen = pdrgubndvInput->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		const CUpperBoundNDVs *pubndv = (*pdrgubndvInput)[ul];
		CDouble dUpperBoundNDVInput = pubndv->DUpperBoundNDVs();
		CDouble dUpperBoundNDVOutput = dRowsOutput;
		if (CStatistics::EcbmInputSourceMaxCard == ecbm)
		{
			dUpperBoundNDVOutput = dUpperBoundNDVInput;
		}
		else if (CStatistics::EcbmMin == ecbm)
		{
			dUpperBoundNDVOutput = std::min(dUpperBoundNDVInput.DVal(), dRowsOutput.DVal());
		}

		CUpperBoundNDVs *pubndvCopy = pubndv->PubndvCopy(pmp, dUpperBoundNDVOutput);
		pstatsOutput->AddCardUpperBound(pubndvCopy);
	}
}

// check if the join statistics object is empty output based on the input
// histograms and the join histograms
BOOL
CJoinStatsProcessor::FEmptyJoinStats
		(
				BOOL fEmptyOuter,
				BOOL fEmptyOutput,
				BOOL fLASJ,
				const CHistogram *phistOuter,
				const CHistogram *phistInner,
				CHistogram *phistJoin
		)
{
	GPOS_ASSERT(NULL != phistOuter);
	GPOS_ASSERT(NULL != phistInner);
	GPOS_ASSERT(NULL != phistJoin);

	return fEmptyOutput ||
		   (!fLASJ && fEmptyOuter) ||
		   (!phistOuter->FEmpty() && !phistInner->FEmpty() && phistJoin->FEmpty());
}

//	return statistics object after performing LOJ operation with another statistics structure
CStatistics *
CJoinStatsProcessor::PstatsLOJStatic
		(
				IMemoryPool *pmp,
				const IStatistics *pstatsOuter,
				const IStatistics *pstatsInner,
				DrgPstatspredjoin *pdrgpstatspredjoin
		)
{
	GPOS_ASSERT(NULL != pstatsOuter);
	GPOS_ASSERT(NULL != pstatsInner);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);

	const CStatistics *pstatsOuterSide = dynamic_cast<const CStatistics *> (pstatsOuter);
	const CStatistics *pstatsInnerSide = dynamic_cast<const CStatistics *> (pstatsInner);

	CStatistics *pstatsInnerJoin = pstatsOuterSide->PstatsInnerJoin(pmp, pstatsInner, pdrgpstatspredjoin);
	CDouble dRowsInnerJoin = pstatsInnerJoin->DRows();
	CDouble dRowsLASJ(1.0);

	// create a new hash map of histograms, for each column from the outer child
	// add the buckets that do not contribute to the inner join
	HMUlHist *phmulhistLOJ = PhmulhistLOJ
			(
					pmp,
					pstatsOuterSide,
					pstatsInnerSide,
					pstatsInnerJoin,
					pdrgpstatspredjoin,
					dRowsInnerJoin,
					&dRowsLASJ
			);

	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);
	CStatisticsUtils::AddWidthInfo(pmp, pstatsInnerJoin->PHMUlDoubleWidth(), phmuldoubleWidth);

	pstatsInnerJoin->Release();

	// cardinality of LOJ is at least the cardinality of the outer child
	CDouble dRowsLOJ = std::max(pstatsOuter->DRows(), dRowsInnerJoin + dRowsLASJ);

	// create an output stats object
	CStatistics *pstatsLOJ = GPOS_NEW(pmp) CStatistics
			(
					pmp,
					phmulhistLOJ,
					phmuldoubleWidth,
					dRowsLOJ,
					pstatsOuter->FEmpty(),
					pstatsOuter->UlNumberOfPredicates()
			);

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated join cardinality.

	// modify source id to upper bound card information
	ComputeCardUpperBounds(pmp, pstatsOuterSide, pstatsLOJ, dRowsLOJ, CStatistics::EcbmMin /* ecbm */);
	pstatsInnerSide->ComputeCardUpperBounds(pmp, pstatsLOJ, dRowsLOJ, CStatistics::EcbmMin /* ecbm */);

	return pstatsLOJ;
}

// return statistics object after performing inner join
CStatistics *
CJoinStatsProcessor::PstatsInnerJoinStatic
		(
				IMemoryPool *pmp,
				const IStatistics *pistatsOuter,
				const IStatistics *pistatsInner,
				DrgPstatspredjoin *pdrgpstatspredjoin
		)
{
	GPOS_ASSERT(NULL != pistatsOuter);
	GPOS_ASSERT(NULL != pistatsInner);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);
	const CStatistics *pstatsOuter = dynamic_cast<const CStatistics *> (pistatsOuter);

	return CJoinStatsProcessor::PstatsJoinDriver
			(
					pmp,
					pstatsOuter->PStatsConf(),
					pistatsOuter,
					pistatsInner,
					pdrgpstatspredjoin,
					IStatistics::EsjtInnerJoin /* esjt */,
					true /* fIgnoreLasjHistComputation */
			);
}

//	create a new hash map of histograms for LOJ from the histograms
//	of the outer child and the histograms of the inner join
HMUlHist *
CJoinStatsProcessor::PhmulhistLOJ
		(
				IMemoryPool *pmp,
				const CStatistics *pstatsOuter,
				const CStatistics *pstatsInner,
				CStatistics *pstatsInnerJoin,
				DrgPstatspredjoin *pdrgpstatspredjoin,
				CDouble dRowsInnerJoin,
				CDouble *pdRowsLASJ
		)
{
	GPOS_ASSERT(NULL != pstatsOuter);
	GPOS_ASSERT(NULL != pstatsInner);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);
	GPOS_ASSERT(NULL != pstatsInnerJoin);

	// build a bitset with all outer child columns contributing to the join
	CBitSet *pbsOuterJoinCol = GPOS_NEW(pmp) CBitSet(pmp);
	for (ULONG ul1 = 0; ul1 < pdrgpstatspredjoin->UlLength(); ul1++)
	{
		CStatsPredJoin *pstatsjoin = (*pdrgpstatspredjoin)[ul1];
		(void) pbsOuterJoinCol->FExchangeSet(pstatsjoin->UlColId1());
	}

	// for the columns in the outer child, compute the buckets that do not contribute to the inner join
	CStatistics *pstatsLASJ = pstatsOuter->PstatsLASJoin
			(
					pmp,
					pstatsInner,
					pdrgpstatspredjoin,
					false /* fIgnoreLasjHistComputation */
			);
	CDouble dRowsLASJ(0.0);
	if (!pstatsLASJ->FEmpty())
	{
		dRowsLASJ = pstatsLASJ->DRows();
	}

	HMUlHist *phmulhistLOJ = GPOS_NEW(pmp) HMUlHist(pmp);

	DrgPul *pdrgpulOuterColId = pstatsOuter->PdrgulColIds(pmp);
	const ULONG ulOuterCols = pdrgpulOuterColId->UlLength();

	for (ULONG ul2 = 0; ul2 < ulOuterCols; ul2++)
	{
		ULONG ulColId = *(*pdrgpulOuterColId)[ul2];
		const CHistogram *phistInnerJoin = pstatsInnerJoin->Phist(ulColId);
		GPOS_ASSERT(NULL != phistInnerJoin);

		if (pbsOuterJoinCol->FBit(ulColId))
		{
			// add buckets from the outer histogram that do not contribute to the inner join
			const CHistogram *phistLASJ = pstatsLASJ->Phist(ulColId);
			GPOS_ASSERT(NULL != phistLASJ);

			if (phistLASJ->FWellDefined() && !phistLASJ->FEmpty())
			{
				// union the buckets from the inner join and LASJ to get the LOJ buckets
				CHistogram *phistLOJ = phistLASJ->PhistUnionAllNormalized(pmp, dRowsLASJ, phistInnerJoin, dRowsInnerJoin);
				CStatisticsUtils::AddHistogram(pmp, ulColId, phistLOJ, phmulhistLOJ);
				GPOS_DELETE(phistLOJ);
			}
			else
			{
				CStatisticsUtils::AddHistogram(pmp, ulColId, phistInnerJoin, phmulhistLOJ);
			}
		}
		else
		{
			// if column from the outer side that is not a join then just add it
			CStatisticsUtils::AddHistogram(pmp, ulColId, phistInnerJoin, phmulhistLOJ);
		}
	}

	pstatsLASJ->Release();

	// extract all columns from the inner child of the join
	DrgPul *pdrgpulInnerColId = pstatsInner->PdrgulColIds(pmp);

	// add its corresponding statistics
	AddHistogramsLOJInner(pmp, pstatsInnerJoin, pdrgpulInnerColId, dRowsLASJ, dRowsInnerJoin, phmulhistLOJ);

	*pdRowsLASJ = dRowsLASJ;

	// clean up
	pdrgpulInnerColId->Release();
	pdrgpulOuterColId->Release();
	pbsOuterJoinCol->Release();

	return phmulhistLOJ;
}


// helper function to add histograms of the inner side of a LOJ
void
CJoinStatsProcessor::AddHistogramsLOJInner
		(
				IMemoryPool *pmp,
				const CStatistics *pstatsInnerJoin,
				DrgPul *pdrgpulInnerColId,
				CDouble dRowsLASJ,
				CDouble dRowsInnerJoin,
				HMUlHist *phmulhistLOJ
		)
{
	GPOS_ASSERT(NULL != pstatsInnerJoin);
	GPOS_ASSERT(NULL != pdrgpulInnerColId);
	GPOS_ASSERT(NULL != phmulhistLOJ);

	const ULONG ulInnerCols = pdrgpulInnerColId->UlLength();

	for (ULONG ul = 0; ul < ulInnerCols; ul++)
	{
		ULONG ulColId = *(*pdrgpulInnerColId)[ul];

		const CHistogram *phistInnerJoin = pstatsInnerJoin->Phist(ulColId);
		GPOS_ASSERT(NULL != phistInnerJoin);

		// the number of nulls added to the inner side should be the number of rows of the LASJ on the outer side.
		CHistogram *phistNull = GPOS_NEW(pmp) CHistogram
				(
						GPOS_NEW(pmp) DrgPbucket(pmp),
						true /*fWellDefined*/,
						1.0 /*dNullFreq*/,
						CHistogram::DDefaultNDVRemain,
						CHistogram::DDefaultNDVFreqRemain
				);
		CHistogram *phistLOJ = phistInnerJoin->PhistUnionAllNormalized(pmp, dRowsInnerJoin, phistNull, dRowsLASJ);
		CStatisticsUtils::AddHistogram(pmp, ulColId, phistLOJ, phmulhistLOJ);

		GPOS_DELETE(phistNull);
		GPOS_DELETE(phistLOJ);
	}
}

// EOF
