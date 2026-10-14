"""자료 오류와 준비 상태의 공통 계약. 원본 예외는 사용자에게 전달하지 않는다."""
from .conversation import DATA_UNAVAILABLE


class SnapshotUnavailable(ValueError):
    MESSAGES = {
        'snapshot_unavailable': DATA_UNAVAILABLE,
        'snapshot_missing': '게시된 삼전점 자료를 찾지 못했습니다. 게시 위치와 동기화 상태를 확인해 주세요.',
        'unsupported_format': '자료는 있지만 현재 지원하는 게시 형식이 아닙니다. 운영자가 확인해야 합니다.',
        'snapshot_invalid': '자료 동기화 또는 검증 문제로 수치 조회를 보류합니다. 운영자가 확인해야 합니다.',
    }

    def __init__(self, message='', code='snapshot_unavailable'):
        self.code = code if code in self.MESSAGES else 'snapshot_invalid'
        super().__init__(self.MESSAGES[self.code])


def readiness(context=None):
    if not context:
        return dict(data_format=None, data_query_ready=False, data_ready=False,
                    available_date_from=None, available_date_to=None, supported_queries=[])
    legacy = context.get('format') == 'legacy_v1'
    return dict(data_format='legacy_v1' if legacy else 'v3', data_query_ready=True,
                data_ready=not legacy, available_date_from=context.get('window_start'),
                available_date_to=context['as_of_date'],
                supported_queries=context.get('supported_queries',
                    ['summary', 'compare', 'anomalies', 'forecast', 'staff_meal', 'evidence', 'status']))
