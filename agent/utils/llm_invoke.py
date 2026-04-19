"""Utility to invoke LLM calls with hard timeout."""
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout


def invoke_with_timeout(callable_fn, timeout_seconds: float):
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(callable_fn)
    try:
        return future.result(timeout=timeout_seconds)
    except FutureTimeout:
        future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise TimeoutError(f"LLM timeout after {timeout_seconds}s")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
