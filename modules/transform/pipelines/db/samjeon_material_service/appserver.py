"""Codex 0.158.0 app-server의 지속 스레드와 동적 도구 연결."""
from .prompt_assets import read_prompt
import asyncio
import json
import os
from pathlib import Path
import subprocess
import time

from .tool_registry import definitions
from .config import local_path
from .wsl_transport import WslTransport

INSTRUCTIONS = read_prompt('conversation')

DAILY_INSTRUCTIONS = read_prompt('daily')


class AppServerError(RuntimeError):
    pass


class AppServer:
    def __init__(self, settings):
        self.settings = settings
        self.process = None
        self.reader = None
        self.pending = {}
        self.events = asyncio.Queue(maxsize=4096)
        self.serial = 0
        self.lock = asyncio.Lock()
        self.work = None
        self.ready = False
        self.last_turn_status = None
        self.last_event_types = []

    async def start(self):
        if self.process and self.process.returncode is None and self.ready:
            return
        await self.close()
        transport = WslTransport(self.settings)
        work = local_path(self.settings.state_root) / 'codex-work'
        work.mkdir(parents=True, exist_ok=True)
        source = Path(__file__).with_name('appserver_runner.py').resolve()
        mapped = []
        for path in (source, work):
            value = await asyncio.to_thread(transport.run, transport.prefix() + ['/usr/bin/wslpath', '-a', '-u', str(path)])
            if value.returncode:
                raise AppServerError('wsl_path_unavailable')
            mapped.append(value.stdout.decode().strip())
        self.work = mapped[1]
        self.events = asyncio.Queue(maxsize=4096)
        self.process = await asyncio.create_subprocess_exec(
            *transport.prefix(), '/usr/bin/python3', '-I', '-B', mapped[0],
            self.settings.wsl_codex_executable, self.work,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, env=transport.environment(),
            limit=2_000_000, creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        self.reader = asyncio.create_task(self._read())
        try:
            await self.rpc('initialize', {'clientInfo': {'name': 'samjeon_telegram', 'title': '삼전점 대화', 'version': '1.0'},
                                         'capabilities': {'experimentalApi': True}})
            await self.send({'method': 'initialized'})
            self.ready = True
        except Exception:
            await self.close()
            raise

    async def _read(self):
        process = self.process
        try:
            while line := await process.stdout.readline():
                message = json.loads(line)
                if 'method' not in message and 'id' in message:
                    future = self.pending.get(message['id'])
                    if future and not future.done():
                        if 'error' in message:
                            future.set_exception(AppServerError('appserver_rpc_failed'))
                        else:
                            future.set_result(message.get('result', {}))
                else:
                    self.events.put_nowait(message)
        except (ValueError, OSError, asyncio.QueueFull):
            pass
        finally:
            self.ready = False
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(AppServerError('appserver_disconnected'))
            try:
                self.events.put_nowait({'method': '_disconnected'})
            except asyncio.QueueFull:
                pass

    async def send(self, value):
        if not self.process or self.process.returncode is not None:
            raise AppServerError('appserver_disconnected')
        self.process.stdin.write((json.dumps(value, ensure_ascii=False) + '\n').encode())
        await self.process.stdin.drain()

    async def rpc(self, method, params, timeout=30):
        self.serial += 1
        identifier = self.serial
        future = asyncio.get_running_loop().create_future()
        self.pending[identifier] = future
        try:
            await self.send({'id': identifier, 'method': method, 'params': params})
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            raise AppServerError('appserver_rpc_timeout') from None
        finally:
            self.pending.pop(identifier, None)

    async def open_thread(self, thread_id=None, *, analysis_only=False):
        await self.start()
        ceo = self.settings.lazy_data
        if ceo:
            from .ceo_tools import INSTRUCTIONS as ceo_instructions
            from .report_schedules import INSTRUCTIONS as schedule_instructions
            ceo_instructions += DAILY_INSTRUCTIONS if analysis_only else schedule_instructions
        common = {'model': self.settings.model, 'cwd': self.work, 'approvalPolicy': 'never',
                  'sandbox': 'read-only',
                  'developerInstructions': ceo_instructions if ceo else DAILY_INSTRUCTIONS if analysis_only else INSTRUCTIONS}
        if not analysis_only:
            common['developerInstructions'] += '\n' + read_prompt('learning') + '\n' + read_prompt('sharing')
        if thread_id:
            result = await self.rpc('thread/resume', {**common, 'threadId': thread_id, 'excludeTurns': True})
        else:
            result = await self.rpc('thread/start', {**common, 'ephemeral': analysis_only,
                                                     'dynamicTools': definitions(self.settings, analysis_only)})
        return result['thread']['id']

    async def turn(self, thread_id, text, tool_call, cancelled, progress=None, context=None, on_started=None):
        async with self.lock:
            while not self.events.empty():
                self.events.get_nowait()
            request = {'threadId': thread_id, 'input': [{'type': 'text', 'text': text}],
                       'effort': self.settings.model_reasoning_effort}
            if context:
                request['input'].insert(0, {'type': 'text', 'text': '서비스가 전달하는 대화 상황(사용자 문맥):\n' +
                                           json.dumps(context, ensure_ascii=False)})
            result = await self.rpc('turn/start', request)
            turn_id = result['turn']['id']
            if on_started:
                on_started(turn_id)
            deadline = time.monotonic() + self.settings.agent_timeout_seconds
            answers, interrupt_at = {}, None
            usage = None
            while True:
                if (cancelled.is_set() or time.monotonic() >= deadline) and interrupt_at is None:
                    cancelled.set()
                    await self.rpc('turn/interrupt', {'threadId': thread_id, 'turnId': turn_id})
                    interrupt_at = time.monotonic()
                if interrupt_at and time.monotonic() - interrupt_at > 10:
                    await self.close()
                    raise AppServerError('cancelled')
                try:
                    message = await asyncio.wait_for(self.events.get(), .2)
                except asyncio.TimeoutError:
                    continue
                method, params = message.get('method'), message.get('params', {})
                if method == 'thread/tokenUsage/updated' and params.get('threadId') == thread_id:
                    usage = params.get('tokenUsage')
                self.last_event_types.append((method, params.get('item', {}).get('type'),
                                              params.get('turn', {}).get('status')))
                self.last_event_types = self.last_event_types[-30:]
                if method == '_disconnected':
                    raise AppServerError('appserver_disconnected')
                if params.get('threadId', thread_id) != thread_id or params.get('turnId', turn_id) != turn_id:
                    if 'id' in message:
                        await self.send({'id': message['id'], 'error': {'code': -32600, 'message': 'Inactive turn'}})
                    continue
                if 'id' in message:
                    if method == 'item/tool/call':
                        arguments = params.get('arguments', {})
                        if isinstance(arguments, str):
                            try:
                                arguments = json.loads(arguments)
                            except ValueError:
                                arguments = {}
                        output = await tool_call(params.get('tool'), arguments)
                        await self.send({'id': message['id'], 'result': {
                            'contentItems': [{'type': 'inputText', 'text': json.dumps(output, ensure_ascii=False)}],
                            'success': 'error' not in output}})
                    else:
                        # 임의 실행·권한 요청은 절대 승인하지 않는다.
                        await self.send({'id': message['id'], 'error': {'code': -32601, 'message': 'Unsupported operation'}})
                    continue
                if method == 'item/completed':
                    item = params.get('item', {})
                    if item.get('type') == 'agentMessage':
                        if item.get('phase') == 'commentary':
                            if progress:
                                await progress(item.get('text', ''))
                        elif item.get('phase') in (None, 'final_answer'):
                            answers[item['id']] = item.get('text', '')
                if method == 'turn/completed':
                    status = params.get('turn', {}).get('status')
                    self.last_turn_status = params.get('turn')
                    if cancelled.is_set() or status == 'interrupted':
                        raise AppServerError('cancelled')
                    if status != 'completed':
                        raise AppServerError('model_failed')
                    answer = '\n\n'.join(answers.values()).strip()
                    if not answer:
                        raise AppServerError('empty_answer')
                    return {'text': answer, 'thread_id': thread_id, 'turn_id': turn_id, 'usage': usage}

    async def close(self):
        self.ready = False
        process, reader = self.process, self.reader
        self.process = self.reader = None
        if process and process.returncode is None:
            process.stdin.close()
            try:
                await asyncio.wait_for(process.wait(), 8)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        if reader and reader is not asyncio.current_task():
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
