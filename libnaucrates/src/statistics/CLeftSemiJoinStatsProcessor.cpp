//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2013 EMC Corp.
//
//	@filename:
//		CLeftSemiJoinStatsProcessor.cpp
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
#include "naucrates/statistics/CLeftSemiJoinStatsProcessor.h"
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

//	return statistics object after performing LSJ operation with another statistics structure
CStatistics *
CLeftSemiJoinStatsProcessor::PstatsLSJoinStatic
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

	const ULONG ulLen = pdrgpstatspredjoin->UlLength();

	// iterate over all inner columns and perform a group by to remove duplicates
	DrgPul *pdrgpulInnerColumnIds = GPOS_NEW(pmp) DrgPul(pmp);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		ULONG ulInnerColId = ((*pdrgpstatspredjoin)[ul])->UlColId2();
		pdrgpulInnerColumnIds->Append(GPOS_NEW(pmp) ULONG(ulInnerColId));
	}

	// dummy agg columns required for group by derivation
	DrgPul *pdrgpulAgg = GPOS_NEW(pmp) DrgPul(pmp);
	IStatistics *pstatsInnerNoDups = pistatsInner->PstatsGroupBy
			(
					pmp,
					pdrgpulInnerColumnIds,
					pdrgpulAgg,
					NULL // pbsKeys: no keys, use all grouping cols
			);

	const CStatistics *pstatsOuter = dynamic_cast<const CStatistics *> (pistatsOuter);
	CStatistics *pstatsSemiJoin = CJoinStatsProcessor::PstatsJoinDriver
			(
					pmp,
					pstatsOuter->PStatsConf(),
					pstatsOuter,
					pstatsInnerNoDups,
					pdrgpstatspredjoin,
					IStatistics::EsjtLeftSemiJoin /* esjt */,
					true /* fIgnoreLasjHistComputation */
			);

	// clean up
	pdrgpulInnerColumnIds->Release();
	pdrgpulAgg->Release();
	pstatsInnerNoDups->Release();

	return pstatsSemiJoin;

}

// EOF
