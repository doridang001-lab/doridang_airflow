"""WSL 안에서 app-server를 격리된 환경으로 실행하고 부모 EOF 시 정리한다."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from modules.transform.pipelines.db.samjeon_material_service.wsl_runner import linux_environment
from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import DISABLED


def main():
    executable, work = sys.argv[1:3]
    env = linux_environment(executable)
    # 향후 사용자 설정에 임의 서버가 추가돼도 업무 세션에 상속하지 않는다.
    import tomllib
    config = Path(env['CODEX_HOME']) / 'config.toml'
    values = tomllib.loads(config.read_text()) if config.exists() else {}
    args = [executable, '--no-daemon', 'app-server', '--listen', 'stdio://',
            '-c', 'web_search="disabled"', '-c', 'project_doc_max_bytes=0',
            '-c', 'suppress_unstable_features_warning=true', '--enable', 'skip_host_skill_discovery']
    for name in DISABLED:
        if name != 'code_mode_host':
            args += ['--disable', name]
    args += ['--enable', 'code_mode_host']
    for name in values.get('mcp_servers', {}):
        args += ['-c', 'mcp_servers.' + json.dumps(name) + '.enabled=false']
    process = subprocess.Popen(args, cwd=work, env=env, start_new_session=True,
                               stdin=subprocess.PIPE, stdout=sys.stdout.buffer, stderr=subprocess.DEVNULL)
    def feed():
        try:
            for line in sys.stdin.buffer:
                process.stdin.write(line)
                process.stdin.flush()
        except (OSError, BrokenPipeError):
            pass
        finally:
            try:
                process.stdin.close()
            except OSError:
                pass
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)

    threading.Thread(target=feed, daemon=True).start()
    try:
        process.wait()
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


if __name__ == '__main__':
    main()
