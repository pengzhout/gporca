//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 EMC Corp.
//
//	@filename:
//		COuterRefStatsProcessor.cpp
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
#include "naucrates/statistics/IStatistics.h"
#include "naucrates/statistics/COuterRefStatsProcessor.h"
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

// helper for deriving statistics for join operation based on given scalar expression
IStatistics *
COuterRefStatsProcessor::PstatsJoinWithOuterRefs
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

	IStatistics::EStatsJoinType eStatsJoinType = IStatistics::EsjtInnerJoin;
	if (COperator::EopLogicalLeftOuterJoin == eopid)
	{
		eStatsJoinType = IStatistics::EsjtLeftOuterJoin;
	}

	// derive stats based on local join condition
	IStatistics *pstatsResult = CJoinStatsProcessor::PstatsJoinArray(pmp, pdrgpstatChildren, pexprScalarLocal, eStatsJoinType);

	if (exprhdl.FHasOuterRefs() && 0 < pdrgpstatOuter->UlLength())
	{
		// derive stats based on outer references
		IStatistics *pstats = PstatsDeriveWithOuterRefs(pmp, exprhdl, pexprScalarOuterRefs, pstatsResult, pdrgpstatOuter, eStatsJoinType);
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
COuterRefStatsProcessor::PstatsDeriveWithOuterRefs
		(
				IMemoryPool *pmp,
				CExpressionHandle &
#ifdef GPOS_DEBUG
				exprhdl // handle attached to the logical expression we want to derive stats for
#endif // GPOS_DEBUG
		,
				CExpression *pexprScalar, // scalar condition to be used for stats derivation
				IStatistics *pstats, // statistics object of the attached expression
				DrgPstat *pdrgpstatOuter, // array of stats objects where outer references are defined
				IStatistics::EStatsJoinType eStatsJoinType
)
{
	GPOS_ASSERT(exprhdl.FHasOuterRefs() && "attached expression does not have outer references");
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != pstats);
	GPOS_ASSERT(NULL != pdrgpstatOuter);
	GPOS_ASSERT(0 < pdrgpstatOuter->UlLength());
	GPOS_ASSERT(IStatistics::EstiSentinel != eStatsJoinType);



	// join outer stats object based on given scalar expression,
	// we use inner join semantics here to consider all relevant combinations of outer tuples
	IStatistics *pstatsOuter = CJoinStatsProcessor::PstatsJoinArray(pmp, pdrgpstatOuter, pexprScalar, IStatistics::EsjtInnerJoin);
	CDouble dRowsOuter = pstatsOuter->DRows();

	// join passed stats object and outer stats based on the passed join type
	DrgPstat *pdrgpstat = GPOS_NEW(pmp) DrgPstat(pmp);
	pdrgpstat->Append(pstatsOuter);
	pstats->AddRef();
	pdrgpstat->Append(pstats);
	IStatistics *pstatsJoined = CJoinStatsProcessor::PstatsJoinArray(pmp, pdrgpstat, pexprScalar, eStatsJoinType);
	pdrgpstat->Release();

	// scale result using cardinality of outer stats and set number of rebinds of returned stats
	IStatistics *pstatsResult = pstatsJoined->PstatsScale(pmp, CDouble(1.0/dRowsOuter));
	pstatsResult->SetRebinds(dRowsOuter);
	pstatsJoined->Release();

	return pstatsResult;
}

// EOF
