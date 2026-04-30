#!/usr/bin/env python3
"""
logger.py 사용 예제

시나리오
────────
1. [스레드 전용] 여러 스레드가 Logger 인스턴스를 공유하며 로깅
2. [멀티프로세싱] Pool 워커 프로세스 내부에서 QueueHandler를 통해 로깅
3. [컨텍스트 매니저] with 블록으로 리스너 자동 종료
4. [예외 로깅] traceback 포함 exception() 사용

파일명 형식: logs/app.{YYYYMMDDHH}  예) logs/app.2026042614
"""

import logging
import multiprocessing
import threading
import time
import random

from src.logger import Logger


LOG_BASE = "logs/app"   # 파일명: logs/app.{YYYYMMDDHH}


def _init_example_worker_logger(
    queue: multiprocessing.Queue,
    name: str = "app",
    level: int = logging.DEBUG,
) -> logging.Logger:
    """예제용 워커 프로세스 logger 초기화."""
    root = logging.getLogger(name)
    root.handlers.clear()
    root.filters.clear()
    root.setLevel(level)
    root.propagate = False
    root.addHandler(logging.handlers.QueueHandler(queue))
    return root


# ══════════════════════════════════════════════════════════════════
# 예제 1 : 멀티스레딩 - Logger 인스턴스를 여러 스레드가 공유
# ══════════════════════════════════════════════════════════════════
def thread_worker(worker_id: int, logger: Logger, iterations: int = 5) -> None:
    for i in range(iterations):
        level = random.choice([logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR])
        logger.log(level, f"[Thread Worker {worker_id}] iteration={i}")
        time.sleep(random.uniform(0.01, 0.05))


def demo_multithreading() -> None:
    print("\n" + "=" * 60)
    print("  예제 1 : 멀티스레딩")
    print("=" * 60)

    logger = Logger(name="thread_demo", log_base=LOG_BASE, level=logging.DEBUG)

    threads = [
        threading.Thread(target=thread_worker, args=(tid, logger), name=f"Worker-{tid}")
        for tid in range(6)
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logger.info("모든 스레드 완료")
    logger.stop()
    print(f"  -> 로그 파일: {LOG_BASE}.{{YYYYMMDDHH}}")


# ══════════════════════════════════════════════════════════════════
# 예제 2 : 멀티프로세싱 - Pool + QueueHandler
# ══════════════════════════════════════════════════════════════════
def process_worker(task_id: int) -> int:
    """Pool 워커: 루트 logger에 QueueHandler 등록"""
    log = logging.getLogger("mp_demo")
    log.info(f"[Process Worker {task_id}] 작업 시작 (PID={multiprocessing.current_process().pid})")

    time.sleep(random.uniform(0.05, 0.2))
    result = task_id * task_id

    log.debug(f"[Process Worker {task_id}] 결과={result}")
    return result


def demo_multiprocessing() -> None:
    print("\n" + "=" * 60)
    print("  예제 2 : 멀티프로세싱 (Pool)")
    print("=" * 60)

    logger = Logger(name="mp_demo", log_base=LOG_BASE, level=logging.DEBUG)

    try:
        with multiprocessing.Pool(
            processes=4,
            initializer=_init_example_worker_logger,
            initargs=(logger.queue, "mp_demo", logging.DEBUG),
        ) as pool:
            tasks = list(range(12))
            results = pool.map(process_worker, tasks)

        logger.info(f"모든 프로세스 완료. 결과 합계={sum(results)}")
        print("  -> 결과:", results)
    finally:
        logger.stop()


# ══════════════════════════════════════════════════════════════════
# 예제 3 : 컨텍스트 매니저 + 프로세스 & 스레드 혼합
# ══════════════════════════════════════════════════════════════════
def mixed_process_task(task_id: int, queue: multiprocessing.Queue) -> None:
    """프로세스 내부에서 스레드도 함께 사용"""
    log = logging.getLogger("mixed")

    def inner_thread(tid: int) -> None:
        log.info(f"  [Process {task_id} / Thread {tid}] 실행 중")
        time.sleep(random.uniform(0.01, 0.05))

    threads = [threading.Thread(target=inner_thread, args=(t,)) for t in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log.info(f"[Process {task_id}] 내부 스레드 모두 완료")


def demo_mixed() -> None:
    print("\n" + "=" * 60)
    print("  예제 3 : 프로세스 + 스레드 혼합 (컨텍스트 매니저)")
    print("=" * 60)

    with Logger(name="mixed", log_base=LOG_BASE, level=logging.DEBUG) as logger:
        processes = [
            multiprocessing.Process(
                target=_mixed_worker_entry,
                args=(pid, logger.queue),
                name=f"MixedProc-{pid}",
            )
            for pid in range(3)
        ]

        for p in processes:
            p.start()
        for p in processes:
            p.join()

        logger.info("예제 3 완료")

    print("  -> 컨텍스트 매니저가 리스너를 자동 정리했습니다.")


def _mixed_worker_entry(task_id: int, queue: multiprocessing.Queue) -> None:
    """multiprocessing.Process target - 최상위 함수여야 pickle 가능"""
    _init_example_worker_logger(queue, name="mixed")
    mixed_process_task(task_id, queue)


# ══════════════════════════════════════════════════════════════════
# 예제 4 : 예외 로깅
# ══════════════════════════════════════════════════════════════════
def demo_exception_logging() -> None:
    print("\n" + "=" * 60)
    print("  예제 4 : 예외(traceback) 로깅")
    print("=" * 60)

    logger = Logger(name="exc_demo", log_base=LOG_BASE)

    try:
        _ = 10 / 0
    except ZeroDivisionError:
        logger.exception("0으로 나누기 에러 발생!")

    try:
        data = {"key": "value"}
        _ = data["missing_key"]
    except KeyError:
        logger.exception("존재하지 않는 키 접근!")

    logger.stop()
    print("  -> 예외 traceback이 로그에 기록되었습니다.")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    if multiprocessing.get_start_method(allow_none=True) is None:
        methods = multiprocessing.get_all_start_methods()
        preferred = "fork" if "fork" in methods else "spawn"
        multiprocessing.set_start_method(preferred)

    demo_multithreading()
    demo_multiprocessing()
    demo_mixed()
    demo_exception_logging()

    print(f"\n모든 예제 완료. 로그 위치: {LOG_BASE}.{{YYYYMMDDHH}}")