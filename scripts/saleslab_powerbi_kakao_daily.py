from __future__ import annotations

import argparse
import ctypes
import io
import os
import subprocess
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from PIL import Image, ImageGrab, ImageStat


POWERBI_URL = (
    "https://app.powerbi.com/groups/me/reports/"
    "b8e556af-8c40-485b-8a9f-691039e312ce/"
    "007d935b0de4a81f5b48"
    "?ctid=849b5035-01a1-48ed-9a23-5bb83c6d0873&experience=power-bi"
)
ROOM_NAME = "\u005b\uc804\ub7b5\uae30\ud68d\ubd80\u005d \ud504\ub2f4CS"
DEFAULT_IMAGE = Path(r"C:\airflow\.tmp\powerbi_saleslab_visible_to_kakao.png")
VK_CONTROL = 0x11
VK_A = 0x41
VK_C = 0x43
VK_F = 0x46
VK_L = 0x4C
VK_V = 0x56
VK_DELETE = 0x2E
VK_ENTER = 0x0D
VK_ESCAPE = 0x1B
VK_MENU = 0x12
VK_F4 = 0x73
VK_HOME = 0x24
VK_PAGEDOWN = 0x22
VK_TAB = 0x09
CF_DIB = 8
GMEM_MOVEABLE = 0x0002


class RECT(ctypes.Structure):
    _fields_ = [
        ("left", ctypes.c_long),
        ("top", ctypes.c_long),
        ("right", ctypes.c_long),
        ("bottom", ctypes.c_long),
    ]


EnumWindowsProc = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def get_window_title(hwnd: int) -> str:
    length = ctypes.windll.user32.GetWindowTextLengthW(hwnd)
    if length <= 0:
        return ""
    buffer = ctypes.create_unicode_buffer(length + 1)
    ctypes.windll.user32.GetWindowTextW(hwnd, buffer, length + 1)
    return buffer.value


def get_rect(hwnd: int) -> tuple[int, int, int, int]:
    rect = RECT()
    if not ctypes.windll.user32.GetWindowRect(hwnd, ctypes.byref(rect)):
        raise RuntimeError("GetWindowRect failed")
    return rect.left, rect.top, rect.right, rect.bottom


def visible_windows() -> list[tuple[int, str, tuple[int, int, int, int]]]:
    windows: list[tuple[int, str, tuple[int, int, int, int]]] = []

    def callback(hwnd, _):
        if ctypes.windll.user32.IsWindowVisible(hwnd):
            title = get_window_title(hwnd)
            windows.append((hwnd, title, get_rect(hwnd)))
        return True

    ctypes.windll.user32.EnumWindows(EnumWindowsProc(callback), 0)
    return windows


def area(rect: tuple[int, int, int, int]) -> int:
    return max(0, rect[2] - rect[0]) * max(0, rect[3] - rect[1])


def find_window_by_title(*needles: str) -> int | None:
    matches = []
    for hwnd, title, rect in visible_windows():
        lower = title.lower()
        if all(needle.lower() in lower for needle in needles):
            matches.append((hwnd, rect))
    if not matches:
        return None
    return sorted(matches, key=lambda item: area(item[1]), reverse=True)[0][0]


def find_kakao_windows() -> list[tuple[int, str, tuple[int, int, int, int]]]:
    result = []
    for hwnd, title, rect in visible_windows():
        if not title:
            continue
        lower = title.lower()
        if "kakaotalk" in lower or "kakao" in lower or "\uce74\uce74\uc624\ud1a1" in title:
            result.append((hwnd, title, rect))
    return sorted(result, key=lambda item: area(item[2]), reverse=True)


def find_window_containing(text: str) -> tuple[int, str, tuple[int, int, int, int]] | None:
    for hwnd, title, rect in visible_windows():
        if title and text in title:
            return hwnd, title, rect
    return None


def activate(hwnd: int, maximize: bool = False) -> tuple[int, int, int, int]:
    ctypes.windll.user32.ShowWindow(hwnd, 3 if maximize else 9)
    time.sleep(0.3)
    ctypes.windll.user32.SetForegroundWindow(hwnd)
    time.sleep(0.6)
    return get_rect(hwnd)


def key_down(vk: int) -> None:
    ctypes.windll.user32.keybd_event(vk, 0, 0, 0)


def key_up(vk: int) -> None:
    ctypes.windll.user32.keybd_event(vk, 0, 0x0002, 0)


def press(vk: int, delay: float = 0.15) -> None:
    key_down(vk)
    key_up(vk)
    time.sleep(delay)


def hotkey(*keys: int, delay: float = 0.2) -> None:
    for key in keys:
        key_down(key)
    for key in reversed(keys):
        key_up(key)
    time.sleep(delay)


def click(x: int, y: int, delay: float = 0.2) -> None:
    ctypes.windll.user32.SetCursorPos(x, y)
    time.sleep(0.08)
    ctypes.windll.user32.mouse_event(0x0002, 0, 0, 0, 0)
    ctypes.windll.user32.mouse_event(0x0004, 0, 0, 0, 0)
    time.sleep(delay)


def paste_text(text: str) -> None:
    import pyperclip

    pyperclip.copy(text)
    hotkey(VK_CONTROL, VK_V)


def paste_text_at(x: int, y: int, text: str) -> None:
    click(x, y)
    hotkey(VK_CONTROL, VK_A, delay=0.1)
    press(VK_DELETE, delay=0.1)
    paste_text(text)
    press(VK_TAB, delay=0.3)


def scroll_wheel(x: int, y: int, amount: int, delay: float = 0.2) -> None:
    ctypes.windll.user32.SetCursorPos(x, y)
    time.sleep(0.08)
    ctypes.windll.user32.mouse_event(0x0002, 0, 0, 0, 0)
    ctypes.windll.user32.mouse_event(0x0004, 0, 0, 0, 0)
    time.sleep(0.08)
    ctypes.windll.user32.mouse_event(0x0800, 0, 0, amount, 0)
    time.sleep(delay)


def find_chrome() -> Path | None:
    candidates = [
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def open_powerbi(wait_seconds: int, maximize: bool) -> tuple[int, int, int, int]:
    chrome = find_chrome()
    if chrome:
        subprocess.Popen([str(chrome), "--new-window", POWERBI_URL])
    else:
        os.startfile(POWERBI_URL)  # type: ignore[attr-defined]
    time.sleep(3)

    hwnd = ctypes.windll.user32.GetForegroundWindow()
    if hwnd is None:
        raise RuntimeError("Power BI 브라우저 창을 찾지 못했습니다.")
    rect = activate(hwnd, maximize=maximize)
    hotkey(VK_CONTROL, VK_L)
    paste_text(POWERBI_URL)
    press(VK_ENTER)
    time.sleep(wait_seconds)
    return activate(hwnd, maximize=maximize)


def date_ranges(base_date: date) -> tuple[date, date, date, date]:
    compare_start = base_date - timedelta(days=3)
    compare_end = base_date - timedelta(days=2)
    current_start = base_date - timedelta(days=1)
    current_end = base_date
    return compare_start, compare_end, current_start, current_end


def set_powerbi_dates(
    browser_rect: tuple[int, int, int, int],
    base_date: date,
    start_offset: tuple[int, int],
    end_offset: tuple[int, int],
    wait_seconds: int,
) -> None:
    left, top, _, _ = browser_rect
    compare_start, compare_end, current_start, current_end = date_ranges(base_date)
    log(
        "조회 범위: "
        f"{compare_start.isoformat()}~{current_end.isoformat()} "
        f"(본기간 {current_start.isoformat()}~{current_end.isoformat()}, "
        f"비교군 {compare_start.isoformat()}~{compare_end.isoformat()})"
    )
    paste_text_at(left + start_offset[0], top + start_offset[1], compare_start.isoformat())
    paste_text_at(left + end_offset[0], top + end_offset[1], current_end.isoformat())
    press(VK_ESCAPE)
    time.sleep(wait_seconds)


def grab(crop: tuple[int, int, int, int]) -> Image.Image:
    virtual_left = ctypes.windll.user32.GetSystemMetrics(76)
    virtual_top = ctypes.windll.user32.GetSystemMetrics(77)
    image_crop = (
        crop[0] - virtual_left,
        crop[1] - virtual_top,
        crop[2] - virtual_left,
        crop[3] - virtual_top,
    )
    return ImageGrab.grab(all_screens=True).crop(image_crop)


def stitch(parts: list[Image.Image], overlap: int) -> Image.Image:
    width = max(part.width for part in parts)
    height = parts[0].height + sum(part.height - overlap for part in parts[1:])
    canvas = Image.new("RGB", (width, height), "white")
    y = 0
    for index, part in enumerate(parts):
        top = overlap if index else 0
        segment = part.crop((0, top, part.width, part.height))
        canvas.paste(segment, (0, y))
        y += segment.height
    return canvas


def is_blank(image: Image.Image) -> bool:
    stat = ImageStat.Stat(image.convert("L"))
    return stat.stddev[0] < 2


def wait_for_report_ready(
    crop: tuple[int, int, int, int],
    timeout_seconds: int,
    settle_seconds: int,
) -> None:
    deadline = time.time() + timeout_seconds
    last_blank = True
    while time.time() < deadline:
        shot = grab(crop)
        blank = is_blank(shot)
        if not blank:
            log(f"리포트 화면 감지, 안정화 대기: {settle_seconds}s")
            time.sleep(settle_seconds)
            if not is_blank(grab(crop)):
                return
        if blank != last_blank:
            log("리포트 화면 로딩 상태 변경 감지")
        last_blank = blank
        time.sleep(2)
    raise RuntimeError(f"Power BI 리포트 화면 로딩 대기 타임아웃: {timeout_seconds}s")


def capture_powerbi(
    output: Path,
    base_date: date,
    wait_seconds: int,
    maximize: bool,
    set_dates: bool,
    crop_offset: tuple[int, int, int, int],
    start_offset: tuple[int, int],
    end_offset: tuple[int, int],
    scroll_click_offset: tuple[int, int],
    page_count: int,
    overlap: int,
    report_ready_timeout: int,
    report_settle_seconds: int,
    reset_scrolls: int,
    wheel_amount: int,
) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    browser_rect = open_powerbi(wait_seconds=wait_seconds, maximize=maximize)

    left, top, _, _ = browser_rect
    crop = (
        left + crop_offset[0],
        top + crop_offset[1],
        left + crop_offset[2],
        top + crop_offset[3],
    )
    scroll_x = left + scroll_click_offset[0]
    scroll_y = top + scroll_click_offset[1]

    log(f"캡처 영역: {crop[0]},{crop[1]},{crop[2]},{crop[3]}")
    if set_dates:
        set_powerbi_dates(browser_rect, base_date, start_offset, end_offset, wait_seconds=1)
        log("날짜 적용 확정 클릭")
        click(scroll_x, scroll_y, delay=1.0)
        log("날짜 적용 후 리포트 로딩 대기")
        wait_for_report_ready(
            crop,
            timeout_seconds=report_ready_timeout,
            settle_seconds=report_settle_seconds,
        )
    else:
        wait_for_report_ready(
            crop,
            timeout_seconds=report_ready_timeout,
            settle_seconds=report_settle_seconds,
        )

    log("리포트 스크롤 상단 초기화")
    for _ in range(reset_scrolls):
        scroll_wheel(scroll_x, scroll_y, abs(wheel_amount), delay=0.15)
    time.sleep(1.0)

    parts = []
    for index in range(page_count):
        parts.append(grab(crop))
        if index < page_count - 1:
            scroll_wheel(scroll_x, scroll_y, -abs(wheel_amount), delay=1.1)

    image = stitch(parts, overlap=overlap)
    if is_blank(image):
        raise RuntimeError("Power BI 캡처 이미지가 비어 있습니다.")
    image.save(output)
    if output.stat().st_size < 20_000:
        raise RuntimeError(f"Power BI 캡처 이미지 크기가 비정상입니다: {output.stat().st_size} bytes")
    log(f"캡처 저장: {output.resolve()} ({output.stat().st_size} bytes)")
    return output


def find_kakaotalk_exe() -> Path | None:
    candidates = [
        Path(os.environ.get("LOCALAPPDATA", "")) / "Kakao" / "KakaoTalk" / "KakaoTalk.exe",
        Path(os.environ.get("ProgramFiles(x86)", "")) / "Kakao" / "KakaoTalk" / "KakaoTalk.exe",
        Path(os.environ.get("ProgramFiles", "")) / "Kakao" / "KakaoTalk" / "KakaoTalk.exe",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def start_kakao() -> None:
    exe = find_kakaotalk_exe()
    if exe:
        subprocess.Popen([str(exe)])
        return
    subprocess.Popen(["explorer.exe", "kakaotalk://"])


def ensure_kakao_ready(timeout_seconds: int) -> tuple[int, int, int, int]:
    start_kakao()
    deadline = time.time() + timeout_seconds
    last_windows: list[tuple[int, str, tuple[int, int, int, int]]] = []
    while time.time() < deadline:
        windows = find_kakao_windows()
        if windows:
            last_windows = windows
            hwnd, title, rect = windows[0]
            log(f"카카오톡 창 감지: {title} {rect}")
            if area(rect) < 90_000:
                press(VK_ENTER, delay=0.8)
                start_kakao()
                time.sleep(2)
                continue
            return activate(hwnd, maximize=True)
        time.sleep(1)

    if last_windows:
        hwnd, _, _ = last_windows[0]
        return activate(hwnd, maximize=True)
    raise RuntimeError("카카오톡 창을 찾지 못했습니다.")


def rect_inside(inner: tuple[int, int, int, int], outer: tuple[int, int, int, int]) -> bool:
    return (
        outer[0] <= inner[0] <= outer[2]
        and outer[1] <= inner[1] <= outer[3]
        and outer[0] <= inner[2] <= outer[2]
        and outer[1] <= inner[3] <= outer[3]
    )


def dismiss_kakao_modal(main_rect: tuple[int, int, int, int]) -> None:
    for _ in range(2):
        closed = False
        for hwnd, title, rect in visible_windows():
            if (
                title
                or not rect_inside(rect, main_rect)
                or area(rect) < 10_000
                or area(rect) > area(main_rect) * 0.75
            ):
                continue
            log(f"카카오톡 모달 닫기 시도: {rect}")
            activate(hwnd, maximize=False)
            press(VK_ESCAPE, delay=0.3)
            click(rect[2] - 18, rect[1] + 18, delay=0.5)
            closed = True
            break
        if not closed:
            return
        time.sleep(0.5)


def copy_image_to_clipboard(path: Path) -> None:
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"클립보드에 넣을 이미지가 없습니다: {resolved}")
    escaped = str(resolved).replace("'", "''")
    subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-Command",
            f"Set-Clipboard -Path '{escaped}'",
        ],
        check=True,
    )


def prepare_kakao_room(
    room_name: str,
    chat_tab_offset: tuple[int, int],
    search_offset: tuple[int, int],
    room_result_offset: tuple[int, int],
    paste_offset: tuple[int, int],
    image_path: Path,
) -> tuple[int, int, int, int]:
    main_rect = ensure_kakao_ready(timeout_seconds=40)
    dismiss_kakao_modal(main_rect)
    press(VK_ESCAPE, delay=0.3)
    left, top, _, _ = main_rect
    click(left + 300, top + 150, delay=0.3)
    click(left + chat_tab_offset[0], top + chat_tab_offset[1], delay=0.4)
    dismiss_kakao_modal(main_rect)
    log(f"카카오톡 방 검색 시작: {room_name}")
    hotkey(VK_CONTROL, VK_F, delay=0.3)
    paste_text(room_name)
    time.sleep(1.0)
    click(left + room_result_offset[0], top + room_result_offset[1], delay=1.2)

    hwnd = ctypes.windll.user32.GetForegroundWindow()
    title = get_window_title(hwnd)
    rect = get_rect(hwnd)
    log(f"채팅창 전면 후보: {title or '<no title>'} {rect}")
    if not title and rect_inside(rect, main_rect) and area(rect) < area(main_rect) * 0.75:
        raise RuntimeError("카카오톡 채팅방 대신 모달/팝업이 전면에 있습니다.")

    room_window = find_window_containing(room_name)
    if not room_window:
        raise RuntimeError(f"카카오톡 방 창을 찾지 못했습니다: {room_name}")

    hwnd, title, rect = room_window
    log(f"채팅창 후보: {title} {rect}")
    rect = activate(hwnd, maximize=True)
    left, top, _, _ = rect

    copy_image_to_clipboard(image_path)
    click(left + paste_offset[0], top + paste_offset[1], delay=0.2)
    hotkey(VK_CONTROL, VK_V, delay=1.5)
    return get_rect(ctypes.windll.user32.GetForegroundWindow())


def send_prepared_image(send_mode: str, send_button_offset: tuple[int, int] | None) -> None:
    if send_mode == "enter":
        press(VK_ENTER, delay=1.0)
        return
    if send_button_offset is None:
        raise RuntimeError("click 전송에는 send_button_offset이 필요합니다.")
    hwnd = ctypes.windll.user32.GetForegroundWindow()
    left, top, _, _ = get_rect(hwnd)
    click(left + send_button_offset[0], top + send_button_offset[1], delay=1.0)


def parse_pair(value: str) -> tuple[int, int]:
    left, right = value.split(",", 1)
    return int(left.strip()), int(right.strip())


def parse_quad(value: str) -> tuple[int, int, int, int]:
    parts = [int(part.strip()) for part in value.split(",")]
    if len(parts) != 4:
        raise ValueError("좌표는 left,top,right,bottom 형식이어야 합니다.")
    return parts[0], parts[1], parts[2], parts[3]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="기준일 YYYY-MM-DD. 기본값은 오늘")
    parser.add_argument("--image", default=str(DEFAULT_IMAGE))
    parser.add_argument("--skip-capture", action="store_true")
    parser.add_argument("--skip-date-input", action="store_true")
    parser.add_argument("--capture-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--send-mode", choices=["enter", "click"], default="enter")
    parser.add_argument("--browser-wait", type=int, default=25)
    parser.add_argument("--no-maximize-browser", action="store_true")
    parser.add_argument("--capture-offset", default="810,120,1810,1390")
    parser.add_argument("--date-start-offset", default="877,284")
    parser.add_argument("--date-end-offset", default="975,284")
    parser.add_argument("--scroll-click-offset", default="1080,720")
    parser.add_argument("--capture-pages", type=int, default=3)
    parser.add_argument("--capture-overlap", type=int, default=160)
    parser.add_argument("--report-ready-timeout", type=int, default=90)
    parser.add_argument("--report-settle-seconds", type=int, default=5)
    parser.add_argument("--reset-scrolls", type=int, default=8)
    parser.add_argument("--wheel-amount", type=int, default=650)
    parser.add_argument("--chat-tab-offset", default="39,140")
    parser.add_argument("--search-offset", default="0,0")
    parser.add_argument("--room-result-offset", default="180,240")
    parser.add_argument("--paste-offset", default="300,1295")
    parser.add_argument("--send-button-offset", default=None)
    args = parser.parse_args()

    base_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
    image_path = Path(args.image)

    if not args.capture_only:
        log("카카오톡 실행 및 최대화")
        ensure_kakao_ready(timeout_seconds=40)

    if not args.skip_capture:
        capture_powerbi(
            output=image_path,
            base_date=base_date,
            wait_seconds=args.browser_wait,
            maximize=not args.no_maximize_browser,
            set_dates=not args.skip_date_input,
            crop_offset=parse_quad(args.capture_offset),
            start_offset=parse_pair(args.date_start_offset),
            end_offset=parse_pair(args.date_end_offset),
            scroll_click_offset=parse_pair(args.scroll_click_offset),
            page_count=args.capture_pages,
            overlap=args.capture_overlap,
            report_ready_timeout=args.report_ready_timeout,
            report_settle_seconds=args.report_settle_seconds,
            reset_scrolls=args.reset_scrolls,
            wheel_amount=args.wheel_amount,
        )

    if not image_path.exists():
        raise FileNotFoundError(f"전송할 이미지가 없습니다: {image_path}")

    if args.capture_only:
        log("capture-only 모드라 카카오톡 준비는 하지 않습니다.")
        return 0

    popup_rect = prepare_kakao_room(
        room_name=ROOM_NAME,
        chat_tab_offset=parse_pair(args.chat_tab_offset),
        search_offset=parse_pair(args.search_offset),
        room_result_offset=parse_pair(args.room_result_offset),
        paste_offset=parse_pair(args.paste_offset),
        image_path=image_path,
    )
    log(f"이미지 첨부 팝업 준비 완료: {popup_rect}")

    if args.dry_run:
        log("dry-run 모드라 실제 전송은 하지 않습니다.")
        return 0

    send_offset = parse_pair(args.send_button_offset) if args.send_button_offset else None
    send_prepared_image(args.send_mode, send_offset)
    log(f"카카오톡 전송 완료: {ROOM_NAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
