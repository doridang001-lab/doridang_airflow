"""전송 방식과 분리된 도구 계약. 실행 주체는 모델 인자가 아닌 서비스가 지정한다."""
from dataclasses import dataclass

CONTRACT_VERSION = 1


@dataclass(frozen=True)
class ActorContext:
    user: int
    chat: int
    assistant: str
    data_scope: str


def definitions(settings, analysis_only=False):
    from .analysis_tools import TOOLS
    from .brain_files import BRAIN_TOOLS
    from .learning import TOOLS as learning
    from .analysis_tools import spec
    from .workspace_files import FILE_TOOLS
    if settings.lazy_data:
        from .ceo_tools import TOOLS
        from .report_schedules import TOOLS as schedules
        base = TOOLS[:4] if analysis_only else TOOLS + schedules + learning
    else:
        base = TOOLS[:3] if analysis_only else TOOLS + learning
    return base + (BRAIN_TOOLS[1:3] if analysis_only else BRAIN_TOOLS + FILE_TOOLS[:2]) + [
        spec('read_assistant_knowledge', '선택한 비서 패키지의 관련 지식·예시를 제한적으로 읽는다.',
             {'query': {'type': 'string'}}, ('query',))]


class ToolRegistry:
    def __init__(self, settings, actor, handler, analysis_only=False):
        self.settings, self.actor, self.handler = settings, actor, handler
        self.specs = {s['name']: s for s in definitions(settings, analysis_only)}

    async def call(self, name, arguments):
        if (self.actor.user not in self.settings.allowed_users
                or self.actor.chat not in self.settings.allowed_chats
                or self.actor.assistant != self.settings.assistant_id
                or self.actor.data_scope != (self.settings.ceo_data_root or self.settings.snapshot_root or self.settings.snapshot)):
            return {'error': 'actor_not_allowed'}
        if name not in self.specs or not isinstance(arguments, dict):
            return {'error': 'tool_not_allowed'}
        schema = self.specs[name]['inputSchema']
        if set(arguments) - set(schema['properties']) or set(schema['required']) - set(arguments):
            return {'error': 'arguments_invalid'}
        return await self.handler(name, arguments)
