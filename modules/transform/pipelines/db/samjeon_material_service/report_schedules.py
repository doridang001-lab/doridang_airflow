"""기존 봇의 개인 대화별 일일 보고 예약과 중복 없는 실행."""
from .prompt_assets import read_prompt
import asyncio
import calendar
from datetime import datetime, timedelta
import json
import math
import re
import threading
from zoneinfo import ZoneInfo

from .analysis_tools import spec
from .brain_files import BrainFiles
from .ceo_tools import CeoTools
from .codex_adapter import redact
from .presentation import telegram_answer

KST = ZoneInfo('Asia/Seoul')
PREFIX = 'report-schedule-v1-'
DEFAULT_PROMPT = ('삼전점 KPI 보고: 직영점_KPI/KPI.md의 목표와 CEO_DB 실적을 확인하고 '
                  '목표·누적 실적·달성률·남은 필요 매출·하루 필요 매출·오늘의 우선순위를 보고해 주세요.')
TOOLS = [
    spec('get_report_schedule', '현재 개인 대화의 매일 반복 보고 예약과 최근 실행 결과를 확인한다.', {}),
    spec('set_report_schedule', '사용자가 직접 요청한 매일 보고를 예약하거나 시간·내용을 수정한다. 시간은 한국시간 HH:MM. 매주·일회성은 지원하지 않는다.',
         {'time': {'type': 'string'}, 'prompt': {'type': 'string'}}, ('time', 'prompt')),
    spec('cancel_report_schedule', '사용자가 직접 요청한 현재 개인 대화의 보고 예약을 해제한다.', {}),
]
INSTRUCTIONS = read_prompt('schedule')
REPORT_INSTRUCTIONS = read_prompt('report')


def next_run(schedule, now):
    if not schedule or not schedule.get('enabled'):
        return None
    now = now.astimezone(KST)
    hour, minute = map(int, schedule['time'].split(':'))
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    first = datetime.fromisoformat(schedule['first_run'])
    return max(target, first).isoformat()


def progress(actual, target, target_month, today):
    """다른 월의 목표를 현재 월 달성률로 계산하지 않는다."""
    if not all(isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)
               for x in (actual, target)) or target <= 0:
        raise ValueError('kpi_amount_invalid')
    result = {'actual_won': actual, 'target_won': target, 'target_month': target_month,
              'attainment_percent': None, 'remaining_won': None, 'required_daily_won': None}
    if target_month != today.strftime('%Y-%m'):
        result['notice'] = '해당 월 목표 없음. 향후 목표 준비 현황이며 달성률을 계산하지 않습니다.'
        return result
    remaining = max(target - actual, 0)
    days = calendar.monthrange(today.year, today.month)[1] - today.day + 1
    return {**result, 'attainment_percent': round(actual / target * 100, 2),
            'remaining_won': remaining, 'remaining_calendar_days': days,
            'required_daily_won': round(remaining / days, 2)}


def kpi_facts(tools, document, today):
    content = document.get('content', '')
    targets = {month: (int(hall.replace(',', '')), int(delivery.replace(',', '')))
               for month, hall, delivery in re.findall(
                   r'(?m)^-\s*(\d{4}-\d{2})\s+홀\s*매출\s+([\d,]+)\s*,\s*배달\s*매출\s+([\d,]+)', content)}
    month = today.strftime('%Y-%m')
    target_month = min((m for m in targets if m >= month), default=None)
    if not target_month:
        return {'notice': '해당 월 및 향후 목표를 문서에서 확인하지 못했습니다.'}
    start, end = today.replace(day=1), today - timedelta(days=1)
    if end < start:
        return {'notice': '월초 첫날로 이번 달 전일까지의 실적이 없습니다. 전월 마감은 별도 집계하세요.'}
    prepared = tools.call('prepare_data', {'dataset': 'samjeon_agent_DB', 'tables': ['00_요약'],
                                         'date_from': str(start), 'date_to': str(end)})
    if 'error' in prepared:
        return {'error': prepared['error'], 'notice': 'KPI 실적 자동 집계 실패. 확인된 자료만 보고하세요.'}
    table = prepared['prepared'][0]
    if not {'홀매출원', '배달매출원', '매출상태'}.issubset(table['columns']):
        return {'error': 'kpi_columns_unavailable'}
    result = tools.call('query_data', {'sql': f'SELECT SUM("홀매출원"), SUM("배달매출원"), '
        f'COUNT(DISTINCT "날짜"), GROUP_CONCAT(DISTINCT "매출상태") FROM {table["sql_table"]}'})
    if 'error' in result or not result.get('rows') or result['rows'][0][0] is None or result['rows'][0][1] is None:
        return {'error': 'kpi_observations_unavailable'}
    hall, delivery, days, status = result['rows'][0]
    return {'source': 'samjeon_agent_DB/00_요약', 'kpi_source': document.get('path'),
            'requested_from': str(start), 'requested_to': str(end),
            'observed_from': table['observed_from'], 'observed_to': table['observed_to'],
            'observed_days': days, 'requested_days': (end-start).days+1, 'sales_status': status,
            'verification': table['verification'], 'notice': table['note'],
            'hall': progress(hall, targets[target_month][0], target_month, today),
            'delivery': progress(delivery, targets[target_month][1], target_month, today),
            'franchise_notice': '가맹점 지표의 정의와 대응 실적은 확인되지 않아 계산하지 않습니다.'}


class Schedules:
    def __init__(self, store, settings):
        self.store, self.settings = store, settings

    def all(self):
        rows = self.store.db.execute('SELECT value FROM meta WHERE key LIKE ?', (PREFIX + '%',)).fetchall()
        return [json.loads(r[0]) for r in rows]

    def recover(self, now):
        for row in self.store.db.execute("SELECT key,value FROM meta WHERE key LIKE 'daily-briefing-v1-%'").fetchall():
            saved = json.loads(row['value'])
            if saved.get('status') == 'generating':
                self.store.put(row['key'], {**saved, 'status': 'interrupted', 'checked_at': now.isoformat()})

    def get(self, chat, now):
        saved = self.store.get(PREFIX + str(chat))
        if not saved:
            return {'enabled': False, 'timezone': 'Asia/Seoul', 'message': '등록된 보고 예약이 없습니다.'}
        last = self.store.get(f'daily-briefing-v1-{chat}-{now.astimezone(KST).date()}')
        return {k: v for k, v in saved.items() if k not in ('user', 'chat')} | {
            'next_run': next_run(saved, now), 'today_run': last}

    def call(self, name, args, *, user, chat, request_id, text, now):
        if user not in self.settings.allowed_users or chat not in self.settings.allowed_chats:
            return {'error': 'schedule_not_authorized'}
        if not self.settings.lazy_data or not isinstance(args, dict):
            return {'error': 'schedule_unavailable'}
        if name == 'get_report_schedule':
            return self.get(chat, now)
        if name not in ('set_report_schedule', 'cancel_report_schedule'):
            return {'error': 'tool_not_allowed'}
        # 권한은 현재 직접 수신한 메시지에서만 얻고 모델·자료의 인자를 믿지 않는다.
        old = self.store.get(PREFIX + str(chat), {})
        topic = re.search(r'예약|매일|정기|보고|브리핑|리포트', text) or (
            old.get('enabled') and re.search(r'시간|오전|오후|\d+\s*시|내용', text))
        verb = re.search(r'해\s*줘|해\s*주세요|해주세요|설정|등록|바꿔|변경|수정|취소|해제|중지|보내|알려|해제해|멈춰|하지\s*마|꺼\s*줘', text)
        cancel = re.search(r'취소|해제|중지|멈춰|하지\s*마|꺼\s*줘', text)
        if (not topic or not verb or (name == 'cancel_report_schedule') != bool(cancel)
                or re.search(r'매주|요일|이번만|한\s*번만|일회성|내일만', text)):
            return {'error': 'explicit_daily_schedule_request_required'}
        if name == 'set_report_schedule' and not (
                re.search(r'예약|매일|앞으로|계속|다음부터|정기', text)
                or old.get('enabled') and re.search(r'시간|오전|오후|\d+\s*시', text)):
            return {'error': 'persistent_schedule_change_requires_explicit_request'}
        op_key = f'report-schedule-op-v1-{chat}-{request_id}'
        previous_op = self.store.get(op_key)
        if previous_op:
            return previous_op
        saved = dict(old)
        if name == 'set_report_schedule':
            time = args.get('time')
            prompt = args.get('prompt')
            if (not isinstance(time, str) or not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d', time)
                    or not isinstance(prompt, str) or not 1 <= len(prompt.strip()) <= 2000):
                return {'error': 'schedule_arguments_invalid'}
            local = now.astimezone(KST)
            hour, minute = map(int, time.split(':'))
            first = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if first <= local:
                first += timedelta(days=1)
            saved.update(time=time, prompt=redact(prompt.strip(), self.settings.token), enabled=True,
                         first_run=first.isoformat())
        else:
            saved['enabled'] = False
        saved.update(user=user, chat=chat, revision=old.get('revision', 0) + 1,
                     changed_at=now.isoformat(), timezone='Asia/Seoul')
        result = {'saved': True, **{k: v for k, v in saved.items() if k not in ('user', 'chat')},
                  'next_run': next_run(saved, now)}
        with self.store.db:
            for key, value in ((PREFIX + str(chat), saved), (op_key, result)):
                self.store.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',
                                      (key, json.dumps(value, ensure_ascii=False)))
        return result

    def claim(self, schedule, now):
        local = now.astimezone(KST)
        if (not schedule.get('enabled') or schedule['user'] not in self.settings.allowed_users
                or schedule['chat'] not in self.settings.allowed_chats):
            return None
        hour, minute = map(int, schedule['time'].split(':'))
        due = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if local < due or due < datetime.fromisoformat(schedule['first_run']):
            return None
        key = f"daily-briefing-v1-{schedule['chat']}-{local.date()}"
        if self.store.outgoing(key):
            return None
        status = 'skipped' if local - due >= timedelta(hours=3) else 'generating'
        with self.store.db:
            count = self.store.db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)', (key, json.dumps({
                'status': status, 'revision': schedule['revision'], 'started_at': local.isoformat()},
                ensure_ascii=False))).rowcount
        return key if count and status == 'generating' else None


async def preview_report(service, schedule):
    """원천 쓰기·발송 없이 생성하며 선택 자료 캐시만 로컬에 남긴다."""
    cancelled = threading.Event()
    tools = CeoTools(service.settings, cancelled=cancelled)
    brain = BrainFiles(service.settings.brain_root)
    kpi = await asyncio.to_thread(brain.call, 'read_brain_file', {'path': '직영점_KPI/KPI.md'})

    async def call(name, args):
        if name in ('data_catalog', 'read_guide', 'prepare_data', 'query_data'):
            return await asyncio.to_thread(tools.call, name, args)
        if name in ('search_brain', 'read_brain_file'):
            return await asyncio.to_thread(brain.call, name, args)
        if name == 'read_assistant_knowledge':
            from .assistant_packages import knowledge
            return knowledge(service.settings, args.get('query'))
        return {'error': 'tool_not_allowed'}

    now = service.clock().astimezone(KST)
    facts = (await asyncio.to_thread(kpi_facts, tools, kpi, now.date())
             if re.search(r'KPI|목표|달성', schedule['prompt'], re.I) else None)
    thread = await service.agent.open_thread(analysis_only=True)
    context = {'request_date': str(now.date()), 'timezone': 'Asia/Seoul', 'scheduled_report': True,
               'date_to': str(now.date() - timedelta(days=1)), 'kpi_document': kpi,
               'verified_kpi_facts': facts}
    from .assistant_packages import context as package_context
    context.update(package_context(service.settings))
    if hasattr(service, 'store') and 'user' in schedule and 'chat' in schedule:
        from .ceo_memory import relevant
        from .learning import Learning
        learning = Learning(service.store, service.settings, schedule['user'], schedule['chat'])
        context.update(learning.context(schedule['prompt']))
        context['remembered_rules'] = relevant(service.store, schedule['user'], schedule['chat'], schedule['prompt'])
        managed = {r['name'] for r in learning.list()}
        context['remembered_rules'] = [r for r in context['remembered_rules'] if r['name'] not in managed]
        context['remembered_rules'] += [{'name': r['name'], 'text': r['content']} for r in context['active_learning']]
        context['conversation_focus'] = service.store.memory.briefing_context(schedule['user'], schedule['chat'], now)
    result = await service.agent.turn(thread, REPORT_INSTRUCTIONS + '\n보고 요청:\n' + schedule['prompt'],
                                      call, cancelled, None, context, lambda _: None)
    if not tools.trace or not tools.context:
        raise ValueError('scheduled_report_without_verified_query')
    answer = telegram_answer(redact(result['text'], service.settings.token))
    if not answer:
        raise ValueError('scheduled_report_empty')
    return {'text': answer, 'as_of_date': tools.context['as_of_date'],
            'snapshot_id': tools.context['snapshot_id'], 'query_count': len(tools.trace),
            'csv_files_read': tools.read_files, 'usage': result.get('usage'),
            'kpi_available': 'error' not in kpi, 'files_written_to_source': False}


async def run_due_reports(service):
    if not service.settings.lazy_data or not service.agent:
        return
    schedules = Schedules(service.store, service.settings)
    for schedule in schedules.all():
        key = schedules.claim(schedule, service.clock())
        if not key:
            continue
        try:
            report = await preview_report(service, schedule)
        except asyncio.CancelledError:
            service.store.put(key, {'status': 'interrupted', 'checked_at': service.clock().isoformat()})
            raise
        except Exception:
            report = {'text': '예약 보고를 완성하지 못했습니다. 자료 검증 또는 AI 연결 상태를 확인해 주세요. 확인되지 않은 수치는 포함하지 않았습니다.',
                      'error': 'report_generation_failed'}
        current = service.store.get(PREFIX + str(schedule['chat']), {})
        if not current.get('enabled') or current.get('revision') != schedule['revision']:
            service.store.put(key, {'status': 'cancelled_before_send', 'checked_at': service.clock().isoformat()})
            continue
        # 대기 중 오래 지난 보고나 다음 날짜의 보고를 뒤늦게 보내지 않는다.
        local = service.clock().astimezone(KST)
        hour, minute = map(int, schedule['time'].split(':'))
        due = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if not key.endswith('-' + str(local.date())) or local - due >= timedelta(hours=3):
            service.store.put(key, {'status': 'skipped_after_generation', 'checked_at': local.isoformat()})
            continue
        try:
            delivered = await service.send(key, schedule['chat'], report['text'])
        except Exception:
            delivered = False
        service.store.put(key, {**{k: v for k, v in report.items() if k != 'text'},
                               'status': 'sent' if delivered else 'undelivered_or_uncertain',
                               'checked_at': service.clock().isoformat()})
        if delivered:
            service.store.put(f"last-scheduled-report-v1-{schedule['user']}-{schedule['chat']}", {
                'text': report['text'][:5000], 'delivered_at': service.clock().isoformat(),
                'as_of_date': report.get('as_of_date'), 'notice': '발송 완료 과거 보고입니다. 현재 수치는 다시 조회하세요.'})
