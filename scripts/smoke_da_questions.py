import json
import time
import requests

BASE_URL = "http://localhost:8000"
ASK_TIMEOUT_SECONDS = 70
EXEC_TIMEOUT_SECONDS = 70

QUESTIONS = [
    "hiện tại database có mấy bảng và dạng dữ liệu của từng cột trong bảng",
    "Tổng doanh thu tháng này",
    "Top 10 khách hàng chi tiêu nhiều nhất",
    "Doanh thu theo khu vực tháng trước",
    "Sản phẩm bán chạy nhất",
    "Tỷ lệ đơn hàng bị hủy",
    "So sánh doanh thu Q1 với Q2",
    "Trung bình giá trị đơn hàng theo tháng",
]


def clip(text: str, n: int = 180) -> str:
    t = (text or "").replace("\n", " ").strip()
    return t[:n]


def main() -> int:
    print("=== PRECHECK ===")
    health = requests.get(f"{BASE_URL}/api/health", timeout=20)
    print("health:", health.status_code, health.text)

    history_check = requests.get(f"{BASE_URL}/api/history?limit=2", timeout=20)
    print("history endpoint:", history_check.status_code)

    if history_check.status_code != 200:
        print("WARN: /api/history is not available on running backend.")

    print("\n=== QUESTION TESTS ===")
    results = []
    for question in QUESTIONS:
        t0 = time.time()
        try:
            ask_resp = requests.post(
                f"{BASE_URL}/api/ask",
                json={"question": question},
                timeout=ASK_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            results.append(
                {
                    "question": question,
                    "ask_status": "EXCEPTION",
                    "ask_latency_ms": int((time.time() - t0) * 1000),
                    "ask_error": clip(str(exc), 180),
                }
            )
            continue

        latency_ms = int((time.time() - t0) * 1000)

        row = {
            "question": question,
            "ask_status": ask_resp.status_code,
            "ask_latency_ms": latency_ms,
        }

        try:
            ask_json = ask_resp.json()
        except Exception:
            ask_json = {"raw": ask_resp.text[:300]}

        row.update(
            {
                "intent": ask_json.get("intent"),
                "requires_approval": ask_json.get("requires_approval"),
                "has_sql": bool(ask_json.get("sql")),
                "has_data": bool(ask_json.get("data")),
                "ask_error": clip(ask_json.get("error", ""), 120),
                "ask_answer": clip(ask_json.get("answer", ""), 200),
            }
        )

        request_id = ask_json.get("request_id")
        if ask_resp.status_code == 200 and ask_json.get("requires_approval") and request_id:
            ex_t0 = time.time()
            try:
                ex_resp = requests.post(
                    f"{BASE_URL}/api/execute",
                    json={"request_id": request_id},
                    timeout=EXEC_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                row.update(
                    {
                        "execute_status": "EXCEPTION",
                        "execute_latency_ms": int((time.time() - ex_t0) * 1000),
                        "execute_error": clip(str(exc), 180),
                    }
                )
                results.append(row)
                continue
            ex_latency = int((time.time() - ex_t0) * 1000)

            try:
                ex_json = ex_resp.json()
            except Exception:
                ex_json = {"raw": ex_resp.text[:300]}

            row.update(
                {
                    "execute_status": ex_resp.status_code,
                    "execute_latency_ms": ex_latency,
                    "execute_has_data": bool(ex_json.get("data")),
                    "execute_error": clip(ex_json.get("error", ""), 120),
                    "execute_answer": clip(ex_json.get("answer", ""), 200),
                }
            )

        results.append(row)

    print(json.dumps(results, ensure_ascii=False, indent=2))

    print("\n=== HISTORY CHECK AFTER TEST ===")
    history_after = requests.get(f"{BASE_URL}/api/history?limit=20", timeout=20)
    print("history status:", history_after.status_code)
    if history_after.status_code == 200:
        items = history_after.json().get("items", [])
        print("history_count:", len(items))
        print("latest_questions:")
        for item in items[:8]:
            print("-", item.get("question"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
