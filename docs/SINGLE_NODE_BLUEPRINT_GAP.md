# Single-Node Blueprint Gap Review (Kur)

## Đã có
- Agentic pipeline cơ bản: intent -> schema -> examples -> generate -> validate -> execute -> format.
- Unity Catalog OSS metadata retrieval + DuckDB execution.
- Approval flow trước khi execute (`/api/ask` -> `/api/execute`).
- Lịch sử chat lưu bền bằng SQLite (`data/history.db`).
- Trino service trong Docker Compose để test single-node.

## So sánh trực tiếp với Genie (single-node scope)

| Năng lực | Kur hiện tại | Genie (reverse-engineered) | Đánh giá |
|---|---|---|---|
| SQL ask/execute có kiểm soát | Có (`requires_approval`, `request_id`) | Có execute tool trong page context | Kur đạt tốt cho POC |
| Intent handling follow-up | Có (`sql_explain`, clarify, meta) | Có, bám page + context platform | Kur ổn nhưng chưa sâu bằng page context |
| Metadata retrieval | UC OSS + physical schema | UC read/search/insights/lineage sâu | Kur thiếu insights/lineage native |
| Tool ecosystem | Tool nội bộ tập trung Text-to-SQL | Khoảng 14 tools, nhiều loại asset | Kur gọn hơn, ít khả năng đa tác vụ |
| Agent routing | Trong 1 app, intent-based | Page-based specialized agents | Kur chưa có handoff đa agent theo page |
| Skills | Markdown skills local | Markdown skills load on-demand | Tương đồng mô hình |
| Persistence/context | SQLite history hội thoại | Context theo asset/page trong workspace | Kur thiếu ngữ cảnh platform-level |
| UX integration | UI web riêng | Tích hợp native Databricks workspace | Kur chưa đạt độ liền mạch sản phẩm |

## Thiếu quan trọng (so với blueprint production-ready)

### 1) Semantic Layer & governance (ưu tiên cao)
- Chưa có semantic model chuẩn (metrics/dimensions/business definitions) dạng YAML/MDL.
- Chưa có verified queries/golden SQL theo domain để giảm silent logic errors.
- Chưa có RBAC thật sự theo user session (hiện tại chưa có authn/authz đầy đủ).

### 2) Reliability & observability (ưu tiên cao)
- Chưa có structured logging + request_id xuyên suốt toàn pipeline.
- Chưa có metrics exporter (latency/error rate/token usage) + dashboard.
- Chưa có dead-letter/error queue cho lỗi lặp lại từ provider/DB.

### 3) Evaluation loop (ưu tiên cao)
- Chưa có bộ câu hỏi benchmark nội bộ + expected SQL/expected result.
- Chưa có offline regression runner cho mỗi thay đổi prompt/agent.
- Chưa có LLM-as-judge hoặc human review workflow cho quality drift.

### 4) Security guardrails (ưu tiên cao)
- Đã có block DDL/DML cơ bản, nhưng chưa có policy engine theo tenant/domain.
- Chưa có rate limit theo user/IP.
- Chưa có secret manager (đang dùng `.env`/settings file).

### 5) Data/engine flexibility (ưu tiên trung bình)
- Trino mới ở mức kết nối + query cơ bản; chưa có connector production (Iceberg/Delta/Hive catalog).
- Chưa có query result cache và schema cache đa engine.
- Chưa có circuit breaker khi engine chậm/lỗi hàng loạt.

### 6) UX parity với Genie (ưu tiên trung bình)
- Chưa có streaming sự kiện bước thực tế từ backend (SSE/WebSocket).
- Chưa có clarification loop nhiều lượt có ngữ cảnh domain (hỏi lại thông minh theo ambiguity).
- Chưa có trusted-answer badge dựa trên verified query.

## Kết luận GAP (single-node)
- Kur hiện tại đã chạm được phần lõi Genie-like cho bài toán Text-to-SQL: ask/execute tách pha, context cơ bản, và execute đa engine (DuckDB/Trino).
- Khoảng cách lớn nhất khi so với Genie là lớp tích hợp nền tảng: lineage/insights, context theo page/asset, và khả năng workflow đa loại tài sản.
- Nếu giữ phạm vi single-node, hướng tối ưu nhất là ưu tiên semantic model + eval pipeline + observability trước khi mở rộng multi-agent/page routing.

## Đề xuất thứ tự triển khai 2 tuần
1. Semantic model + verified queries theo 1 domain (Sales).
2. Evaluation set 100 câu + regression script tự động.
3. Observability tối thiểu: request_id, JSON logs, metrics endpoint.
4. Auth đơn giản + RBAC read-only theo role.
5. Streaming step events (SSE) để thay loading giả lập.
6. Nâng Trino connector từ memory sang Iceberg/Hive catalog thực tế.
