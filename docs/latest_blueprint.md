# Kur Agentic Text-to-SQL — Latest Blueprint (Current State)

Updated: 2026-04-19
Status: Active development, stable for internal demo and iterative hardening.

## 0) Executive Snapshot
- Codebase đã ổn định theo cấu trúc module: `app` (API), `agent` (LLM/LangGraph), `ui` (static frontend), `docs`.
- Luồng chính là **approval-first**:
  - `POST /api/ask`: chuẩn bị SQL, trả `requires_approval=true`.
  - `POST /api/execute`: chạy SQL theo `request_id`.
- UI hiện tại là **single-flow response** (không còn Phase blocks).
- Backend đã chặn hành vi trả lời “đoán trước kết quả”; câu trả lời sau execute được build từ data thực tế.
- Phoenix + OpenTelemetry đã gắn ở API layer.

## 1) Runtime Architecture
1. Frontend (Vanilla HTML/JS, no-build)
   - UI enterprise light/dark, chat-centric.
   - Side drawer settings cho Router/Generator, DB engine, Polaris, Agent behavior.
2. Backend (FastAPI)
   - Endpoints: health/settings/history/suggestions/ask/execute.
   - Quản lý pending query TTL + chat history persistence.
3. Agent orchestration (LangGraph)
   - Router model định tuyến SQL/chat.
   - Generator model hỗ trợ tool/schema cho SQL path.
4. Catalog + execution
   - Polaris endpoint/configuration managed qua settings.
   - DuckDB execution path là mặc định; Trino có path cấu hình.
5. Observability
   - OpenTelemetry + Phoenix instrumentation tại API startup.

## 2) Interaction Flow (As-is)
### 2.1 Ask flow (`/api/ask`)
- Nếu là câu hỏi liệt kê bảng: trả trực tiếp danh sách bảng, không generate SQL.
- Nếu là follow-up explain SQL: lấy SQL gần nhất (hoặc SQL inline) rồi trả giải thích.
- Nếu nhận diện được quick SQL từ schema adapter: chuẩn bị SQL ngay.
- Nếu đi qua agent path: trích SQL từ markdown block, normalize rồi chờ approval.
- Với trường hợp có SQL và chưa auto execute: luôn trả thông điệp trung tính “đã chuẩn bị SQL”.

### 2.2 Execute flow (`/api/execute`)
- Resolve `request_id`, chạy SQL trên engine hiện hành.
- Trả `data`, `columns`, `latency_ms`.
- Tạo câu trả lời business-friendly từ kết quả thực (`_build_grounded_answer_from_result`) thay vì prose suy diễn trước đó.

## 3) UI Behavior (As-is)
- Header: Kur AI, trạng thái engine, actions (new chat/theme/show SQL/traces/settings).
- Chat:
  - User bubble bên phải.
  - Assistant card bên trái.
  - Loading dạng “Thinking...” + 3 dots.
  - SQL block dạng collapsible + copy.
  - Data table render tối đa 100 dòng trên UI.
- Approval actions:
  - `Allow chạy query` gọi `/api/execute`.
  - `Skip` chỉ bỏ qua request pending hiện tại.

## 4) Persistence & Config
- Settings lưu ở `data/settings.json`.
- History lưu ở `data/history.db`.
- Các key/config được map sang env runtime qua `app/core/config.py`.
- Pending approvals giữ in-memory (`PENDING_QUERIES`) với TTL 1800 giây.

## 5) Completed Hardening
- Đã modularize frontend (`ui/app.js`, `ui/js/api.js`, `ui/js/chat.js`, `ui/js/settings.js`).
- Đã thêm no-cache strategy cho static assets + version bump để tránh stale UI.
- Đã loại bỏ Phase rendering cũ trong chat.
- Đã thêm grounded-answer rule để đồng bộ answer với query result thực.
- Đã thêm intent guard cho schema-list và SQL explain follow-up.

## 6) Current Constraints
- Grounded summarizer hiện dựa trên heuristic của first-row/known column names; chưa phải semantic layer đầy đủ.
- UI chưa stream timeline tool-calls theo span trong chat canvas.
- SQL syntax highlight ở mức styled block, chưa token-level highlighting.
- DDL/DML guardrails đang nằm phân tán theo flow; chưa thống nhất thành policy engine tập trung.

## 7) Verification Checklist
1. `GET /api/health` trả engine và số bảng hợp lệ.
2. `POST /api/ask` với câu SQL-query trả `requires_approval=true`, không phán đoán kết quả trước execute.
3. `POST /api/execute` trả answer khớp `data/columns` thực tế.
4. Câu hỏi “liệt kê các bảng” trả danh sách bảng trực tiếp.
5. Câu hỏi follow-up explain SQL bám SQL gần nhất trong history.

## 8) Next Iteration Candidates
1. Chuẩn hóa grounded summarizer theo semantic metadata thay vì heuristic cột.
2. Đồng bộ trace_id/span_id từ backend sang UI.
3. Bổ sung syntax highlighting token-level cho SQL block.
