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


// helper for deriving statistics for join operation based on given scalar expression
IStatistics *
CJoinStatsProcessor::PstatsJoinWithOuterRefs
		(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				DrgPstat *pdrgpstatChildren,
				CExpression *pexprScalarLocal, // filter expression on local columns only
				CExpression *pexprScalarOuterRefs, // filter expression involving outer references
				DrgPstat *pdrgpstatOuter
		)
{
	GPOS_ASSERT(NULL != pdrgpstatChildren);
	GPOS_ASSERT(NULL != pexprScalarLocal);
	GPOS_ASSERT(NULL != pexprScalarOuterRefs);
	GPOS_ASSERT(NULL != pdrgpstatOuter);

	COperator::EOperatorId eopid = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopLogicalLeftOuterJoin == eopid ||
				COperator::EopLogicalInnerJoin == eopid ||
				COperator::EopLogicalNAryJoin == eopid);

	BOOL fOuterJoin = (COperator::EopLogicalLeftOuterJoin == eopid);

	// TODO: Melanie add PstatsJoinArray to the PSt
	// derive stats based on local join condition
	IStatistics *pstatsResult = CJoinStatsProcessor::PstatsJoinArray(pmp, fOuterJoin, pdrgpstatChildren, pexprScalarLocal);

	if (exprhdl.FHasOuterRefs() && 0 < pdrgpstatOuter->UlLength())
	{
		// derive stats based on outer references
		IStatistics *pstats = PstatsDeriveWithOuterRefs(pmp, fOuterJoin, exprhdl, pexprScalarOuterRefs, pstatsResult, pdrgpstatOuter);
		pstatsResult->Release();
		pstatsResult = pstats;
	}

	return pstatsResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsUtils::PstatsDeriveWithOuterRefs
//
//	@doc:
//		Derive statistics when scalar expression uses outer references,
//		we pull statistics for outer references from the passed statistics
//		context, and use Join statistics derivation in this case
//
//		For example:
//
//			Join
//			 |--Get(R)
//			 +--Join
//				|--Get(S)
//				+--Select(T.t=R.r)
//					+--Get(T)
//
//		when deriving statistics on 'Select(T.t=R.r)', we join T with the
//		cross product (R x S) based on the condition (T.t=R.r)
//
//---------------------------------------------------------------------------
IStatistics *
CJoinStatsProcessor::PstatsDeriveWithOuterRefs
		(
				IMemoryPool *pmp,
				BOOL fOuterJoin,
				CExpressionHandle &
#ifdef GPOS_DEBUG
				exprhdl // handle attached to the logical expression we want to derive stats for
#endif // GPOS_DEBUG
		,
				CExpression *pexprScalar, // scalar condition to be used for stats derivation
				IStatistics *pstats, // statistics object of the attached expression
				DrgPstat *pdrgpstatOuter // array of stats objects where outer references are defined
)
{
	GPOS_ASSERT(exprhdl.FHasOuterRefs() && "attached expression does not have outer references");
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != pstats);
	GPOS_ASSERT(NULL != pdrgpstatOuter);
	GPOS_ASSERT(0 < pdrgpstatOuter->UlLength());

	// join outer stats object based on given scalar expression,
	// we use inner join semantics here to consider all relevant combinations of outer tuples
	IStatistics *pstatsOuter = CJoinStatsProcessor::PstatsJoinArray(pmp, false /*fOuterJoin*/, pdrgpstatOuter, pexprScalar);
	CDouble dRowsOuter = pstatsOuter->DRows();

	// join passed stats object and outer stats based on the passed join type
	DrgPstat *pdrgpstat = GPOS_NEW(pmp) DrgPstat(pmp);
	pdrgpstat->Append(pstatsOuter);
	pstats->AddRef();
	pdrgpstat->Append(pstats);
	IStatistics *pstatsJoined = CJoinStatsProcessor::PstatsJoinArray(pmp, fOuterJoin, pdrgpstat, pexprScalar);
	pdrgpstat->Release();

	// scale result using cardinality of outer stats and set number of rebinds of returned stats
	IStatistics *pstatsResult = pstatsJoined->PstatsScale(pmp, CDouble(1.0/dRowsOuter));
	pstatsResult->SetRebinds(dRowsOuter);
	pstatsJoined->Release();

	return pstatsResult;
}


//		 Derive statistics for join operation given array of statistics object
IStatistics *
CJoinStatsProcessor::PstatsJoin
		(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				DrgPstat *pdrgpstatCtxt
		)
{
	GPOS_ASSERT(CLogical::EspNone < CLogical::PopConvert(exprhdl.Pop())->Esp(exprhdl));

	DrgPstat *pdrgpstat = GPOS_NEW(pmp) DrgPstat(pmp);
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		IStatistics *pstatsChild = exprhdl.Pstats(ul);
		pstatsChild->AddRef();
		pdrgpstat->Append(pstatsChild);
	}

	CExpression *pexprJoinPred = NULL;
	if (exprhdl.Pdpscalar(ulArity - 1)->FHasSubquery())
	{
		// in case of subquery in join predicate, assume join condition is True
		pexprJoinPred = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
	}
	else
	{
		// remove implied predicates from join condition to avoid cardinality under-estimation
		pexprJoinPred = CPredicateUtils::PexprRemoveImpliedConjuncts(pmp, exprhdl.PexprScalarChild(ulArity - 1), exprhdl);
	}

	// split join predicate into local predicate and predicate involving outer references
	CExpression *pexprLocal = NULL;
	CExpression *pexprOuterRefs = NULL;

	// get outer references from expression handle
	CColRefSet *pcrsOuter = exprhdl.Pdprel()->PcrsOuter();

	CPredicateUtils::SeparateOuterRefs(pmp, pexprJoinPred, pcrsOuter, &pexprLocal, &pexprOuterRefs);
	pexprJoinPred->Release();

	IStatistics *pstatsJoin = CJoinStatsProcessor::PstatsJoinWithOuterRefs(pmp, exprhdl, pdrgpstat, pexprLocal, pexprOuterRefs, pdrgpstatCtxt);
	pexprLocal->Release();
	pexprOuterRefs->Release();

	pdrgpstat->Release();

	return pstatsJoin;
}

// EOF
