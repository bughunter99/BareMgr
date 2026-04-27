#!/usr/bin/env python3
"""
Thread-safe and process-safe Logger.

Architecture
────────────
  logger.info(msg)
       │
       ▼
  QueueHandler  ──►  multiprocessing.Queue
                              │
                              ▼
                     [리스너 프로세스]
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
             콘솔(stdout)        HourlyFileHandler
                              파일명: {base}.{YYYYMMDDHH}
                              예)  app.2026042614

- 멀티스레딩: 모든 스레드가 동일 QueueHandler를 사용 → 큐에서 직렬화
- 멀티프로세싱: 자식 프로세스도 같은 큐를 공유 → init_worker_logger() 사용
"""

import logging
import logging.handlers
import multiprocessing
import threading
import sys
from datetime import datetime
from pathlib import Path


# ──────────────────────────────────────────────
# Log level 상수
# ──────────────────────────────────────────────
DEBUG    = logging.DEBUG
INFO     = logging.INFO
WARNING  = logging.WARNING
ERROR    = logging.ERROR
CRITICAL = logging.CRITICAL


# ══════════════════════════════════════════════
# HourlyFileHandler
# ══════════════════════════════════════════════
class HourlyFileHandler(logging.Handler):
    """
    시간(hour) 단위로 파일을 교체하는 핸들러.
    파일명: {base_path}.{YYYYMMDDHH}
    예)     logs/app.2026042614
    """

    def __init__(self, base_path: str, encoding: str = "utf-8") -> None:
        super().__init__()
        self._base_path = base_path
        self._encoding  = encoding
        self._lock      = threading.Lock()
        self._current_suffix: str = ""
        self._stream = None
        self._rotate()  # 시작 시 즉시 파일 열기

    # ── 내부 ────────────────────────────────
    def _hour_suffix(self) -> str:
        return datetime.now().strftime("%Y%m%d%H")

    def _rotate(self) -> None:
        """현재 시간에 맞는 파일을 연다."""
        suffix = self._hour_suffix()
        if suffix == self._current_suffix:
            return
        if self._stream:
            try:
                self._stream.flush()
                self._stream.close()
            except OSError:
                pass
        path = Path(f"{self._base_path}.{suffix}")
        path.parent.mkdir(parents=True, exist_ok=True)
        self._stream = open(path, "a", encoding=self._encoding)
        self._current_suffix = suffix

    # ── public ──────────────────────────────
    def emit(self, record: logging.LogRecord) -> None:
        try:
            with self._lock:
                self._rotate()
                msg = self.format(record)
                self._stream.write(msg + "\n")
                self._stream.flush()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        with self._lock:
            if self._stream:
                try:
                    self._stream.flush()
                    self._stream.close()
                except OSError:
                    pass
                self._stream = None
        super().close()

    @property
    def current_file(self) -> str:
        """현재 쓰고 있는 파일 경로를 반환한다."""
        return f"{self._base_path}.{self._current_suffix}"


# ══════════════════════════════════════════════
# Logger
# ══════════════════════════════════════════════
class Logger:
    """
    멀티프로세싱 / 멀티스레딩 환경에서 안전하게 사용할 수 있는 Logger.

    내부 구조
    ─────────
    · 모든 로그 호출은 QueueHandler를 통해 multiprocessing.Queue로 전달된다.
    · 리스너 프로세스가 큐에서 LogRecord를 꺼내 콘솔 + HourlyFileHandler에 기록한다.
    · 파일명 형식: {log_base}.{YYYYMMDDHH}   예) logs/app.2026042614

    사용 예
    ───────
    logger = Logger(name="app", log_base="logs/app")
    # 리스너는 __init__에서 자동 시작된다.

    logger.info("메시지")
    logger.error("에러")

    # 자식 프로세스
    pool = multiprocessing.Pool(
        initializer=init_worker_logger,
        initargs=(logger.queue, "app"),
    )

    # 종료
    logger.stop()
    """

    def __init__(
        self,
        name: str      = "app",
        log_base: str  = "logs/app",
        level: int     = logging.DEBUG,
        fmt: str       = (
            "%(asctime)s [%(levelname)-8s] "
            "[PID:%(process)d TID:%(thread)d] "
            "%(name)s - %(message)s"
        ),
        encoding: str  = "utf-8",
        console: bool  = True,
    ) -> None:
        self.name     = name
        self.log_base = log_base
        self.level    = level
        self.fmt      = fmt
        self.encoding = encoding
        self.console  = console

        # ── 큐 ────────────────────────────────
        self._queue: multiprocessing.Queue = multiprocessing.Queue(-1)

        # ── 리스너 프로세스 ────────────────────
        self._listener: multiprocessing.Process = multiprocessing.Process(
            target=_listener_worker,
            args=(self._queue, name, level, fmt, log_base, encoding, console),
            daemon=True,
            name=f"{name}-log-listener",
        )
        self._listener.start()

        # ── 이 프로세스용 내부 logger (QueueHandler만 사용) ─
        self._logger = self._build_queue_logger()

    # ── 내부 ────────────────────────────────────
    def _build_queue_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        logger.propagate = False
        logger.handlers.clear()
        logger.addHandler(logging.handlers.QueueHandler(self._queue))
        return logger

    # ── 프로퍼티 ─────────────────────────────────
    @property
    def queue(self) -> multiprocessing.Queue:
        """자식 프로세스에 큐를 넘길 때 사용."""
        return self._queue

    # ── 종료 ─────────────────────────────────────
    def stop(self, timeout: float = 5.0) -> None:
        """리스너 프로세스를 정상 종료한다."""
        self._queue.put_nowait(None)   # sentinel
        self._listener.join(timeout=timeout)
        if self._listener.is_alive():
            self._listener.terminate()

    # ── 컨텍스트 매니저 ──────────────────────────
    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.stop()

    # ── 로깅 메서드 ──────────────────────────────
    def debug(self, msg: str, *args, **kwargs) -> None:
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs) -> None:
        self._logger.critical(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs) -> None:
        self._logger.exception(msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args, **kwargs) -> None:
        self._logger.log(level, msg, *args, **kwargs)


# ══════════════════════════════════════════════
# 리스너 프로세스 워커 (최상위 함수 – pickle 가능)
# ══════════════════════════════════════════════
def _listener_worker(
    queue: multiprocessing.Queue,
    name: str,
    level: int,
    fmt: str,
    log_base: str,
    encoding: str,
    console: bool,
) -> None:
    """
    큐에서 LogRecord를 꺼내 핸들러에 전달한다.
    None(sentinel)을 받으면 종료한다.
    """
    formatter = logging.Formatter(fmt)
    handlers: list[logging.Handler] = []

    if console:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        ch.setLevel(level)
        handlers.append(ch)

    fh = HourlyFileHandler(base_path=log_base, encoding=encoding)
    fh.setFormatter(formatter)
    fh.setLevel(level)
    handlers.append(fh)

    while True:
        try:
            record = queue.get(block=True)
            if record is None:   # sentinel → 종료
                break
            for h in handlers:
                if record.levelno >= h.level:
                    h.handle(record)
        except (KeyboardInterrupt, SystemExit):
            break
        except Exception:
            import traceback
            traceback.print_exc(file=sys.stderr)

    for h in handlers:
        h.close()


# ══════════════════════════════════════════════
# 자식 프로세스 초기화 헬퍼
# ══════════════════════════════════════════════
def init_worker_logger(
    queue: multiprocessing.Queue,
    name: str  = "app",
    level: int = logging.DEBUG,
) -> logging.Logger:
    """
    multiprocessing.Pool(initializer=...) 또는 Process 내부에서 호출.
    자식 프로세스의 logger를 QueueHandler만 사용하도록 초기화한다.

    사용 예:
        pool = multiprocessing.Pool(
            initializer=init_worker_logger,
            initargs=(logger.queue, "app"),
        )
    """
    root = logging.getLogger(name)
    root.handlers.clear()
    root.setLevel(level)
    root.propagate = False
    root.addHandler(logging.handlers.QueueHandler(queue))
    return root
