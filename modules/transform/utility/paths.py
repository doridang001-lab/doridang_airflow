from pathlib import Path
import os
import platform

# 우선순위에 따라 OneDrive 경로 결정
# 1) 환경변수 지정 (절대경로)
# 2) 컨테이너 마운트 경로 (/opt/airflow/Doridang) 존재 시
# 3) Windows 로컬 사용자 OneDrive 경로
# 4) 마지막으로 홈 디렉터리 내 OneDrive 추정 경로


def resolve_onedrive_db() -> Path:
	# 1) 환경변수 우선
	env_path = os.getenv("ONEDRIVE_DB")
	if env_path:
		p = Path(env_path)
		return p

	# 2) 컨테이너(Bind mount) 경로
	container_mount = Path("/opt/airflow/Doridang")
	if container_mount.exists():
		return container_mount

	# 3) Windows 로컬 경로
	if platform.system() == "Windows":
		user_home = Path.home()
		win_onedrive = user_home / "OneDrive - 주식회사 도리당" / "Doridang"
		if win_onedrive.exists():
			return win_onedrive

	# 4) 홈 디렉터리 추정
	fallback = Path.home() / "OneDrive - 주식회사 도리당" / "Doridang"
	return fallback


def resolve_collect_db() -> Path:
	# 1) 환경변수 지정
	env_path = os.getenv("COLLECT_DB")
	if env_path:
		return Path(env_path)
	# 2) 컨테이너 마운트 경로
	container_mount = Path("/opt/airflow/Collect_Data")
	if container_mount.exists():
		return container_mount
	# 3) Windows 로컬 경로 (OneDrive 내부)
	if platform.system() == "Windows":
		user_home = Path.home()
		win_collect = user_home / "OneDrive - 주식회사 도리당" / "Collect_Data"
		if win_collect.exists():
			return win_collect
	# 4) Fallback
	return Path.home() / "OneDrive - 주식회사 도리당" / "Collect_Data"


def resolve_local_db() -> Path:
	"""로컬 DB 경로 설정 (충돌 방지를 위한 로컬 저장소)"""
	# 1) 환경변수 우선 (Docker 컨테이너)
	env_path = os.getenv("LOCAL_DB")
	if env_path:
		p = Path(env_path)
		p.mkdir(parents=True, exist_ok=True)
		return p
	
	# 2) 컨테이너 마운트 경로
	container_mount = Path("/opt/airflow/Local_DB")
	if container_mount.parent.exists():  # /opt/airflow가 있으면 컨테이너 환경
		container_mount.mkdir(parents=True, exist_ok=True)
		return container_mount
	
	# 3) Windows 로컬 경로
	if platform.system() == "Windows":
		local_db = Path("C:/Local_DB")
		local_db.mkdir(parents=True, exist_ok=True)
		return local_db
	
	# 4) Fallback - 현재 작업 디렉터리
	fallback = Path.cwd() / "Local_DB"
	fallback.mkdir(parents=True, exist_ok=True)
	return fallback


def resolve_temp_dir() -> Path:
	"""임시 작업 디렉터리 경로.

	우선순위:
	1) 환경변수 `TEMP_DIR`
	2) 컨테이너 기본 경로 `/opt/airflow/Doridang/temp`
	3) Windows 기본 경로 `C:/Doridang/temp`
	4) 현재 작업 디렉터리 하위 `Doridang/temp`
	"""
	# 1) 환경변수 우선
	env_path = os.getenv("TEMP_DIR")
	if env_path:
		p = Path(env_path)
		p.mkdir(parents=True, exist_ok=True)
		return p

	# 2) 컨테이너 기본 경로
	container_default = Path("/opt/airflow/Doridang/temp")
	if container_default.parent.parent.exists():  # /opt/airflow 존재 시 컨테이너 환경으로 가정
		container_default.mkdir(parents=True, exist_ok=True)
		return container_default

	# 3) Windows 기본 경로
	if platform.system() == "Windows":
		win_temp = Path("C:/Local_DB/temp")
		win_temp.mkdir(parents=True, exist_ok=True)
		return win_temp

	# 4) Fallback
	fallback = Path.cwd() / "Doridang" / "temp"
	fallback.mkdir(parents=True, exist_ok=True)
	return fallback


ONEDRIVE_DB = resolve_onedrive_db()
COLLECT_DB = resolve_collect_db()
LOCAL_DB = resolve_local_db()
TEMP_DIR = resolve_temp_dir()
