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
#include "naucrates/statistics/CJoinStatsProcessor.h"
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





// return statistics object after performing inner join
CStatistics *
CInnerJoinStatsProcessor::PstatsInnerJoinStatic
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

// EOF
