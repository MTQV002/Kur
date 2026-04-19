# Kur Frontend UI/UX Blueprint (As-built)
*Last updated: 2026-04-19*

Tài liệu này mô tả **trạng thái UI hiện tại đang chạy** của Kur, không phải backlog mong muốn.

## 1) Design Intent (Đang áp dụng)
- **Ultra-clean B2B:** ưu tiên readability, loại bỏ yếu tố trang trí không phục vụ dữ liệu.
- **Data-first:** câu trả lời, SQL và bảng kết quả là trung tâm.
- **Single-flow Assistant:** không hiển thị “Phase 1/Phase 2”; phản hồi đi theo một mạch.
- **Low-friction Settings:** cấu hình model/engine/catalog trong side drawer, không rời màn hình chat.

## 2) Visual System (Đang áp dụng)
- **Typography:** Inter + JetBrains Mono cho SQL/code.
- **Iconography:** Lucide icons đồng nhất.
- **Theme:** light enterprise mặc định + dark theme toggle.
- **Primary accents:**
  - Light: nhóm xanh enterprise (`#1E40AF`)
  - Dark: nhóm tím enterprise (`#6C5CE7`)
- **Surface model:** phân lớp bằng nền + border; shadow nhẹ để tránh “nặng UI”.

## 3) Current Layout
### A. Header
- Logo text: **Kur AI**.
- DB status: `● Connected: <ENGINE>`.
- Controls: New chat, Theme toggle, Show SQL toggle, Traces link, Settings.

### B. Chat Canvas
- **User message:** bubble phải.
- **Assistant message:** card trái, nội dung single-flow.
- **Loading:** `Thinking...` + 3-dot animation.
- **Response order:**
  1) `answer` text
  2) SQL card (nếu bật show SQL và có SQL)
  3) Data table (nếu có data)
  4) Approval actions (nếu `requires_approval=true`)
  5) Latency badge

### C. SQL + Table Blocks
- SQL dùng `<details>` có caret mở/đóng.
- Có nút Copy SQL.
- Bảng kết quả sticky header, giới hạn render tối đa 100 dòng trên UI.

### D. Settings Drawer
- Slide-over từ bên phải.
- Tabs hiện tại:
  1. Model Routing
  2. Analytics Engine
  3. Iceberg Catalog
  4. Agent Behavior
- Hỗ trợ provider/model/API key cho Router và Generator độc lập.

## 4) Implementation Notes (Khớp code hiện tại)
- Frontend đã modular hóa:
  - `ui/app.js`: bootstrap + theme + app state
  - `ui/js/api.js`: health/suggestions/history
  - `ui/js/chat.js`: render chat + ask/execute flow
  - `ui/js/settings.js`: drawer + save/reset settings
- Đã bật cache-busting assets (`v=4.2`) và no-cache cho HTML/CSS/JS qua nginx config.

## 5) Non-goals in current UI
- Chưa stream timeline nội bộ từng tool-call trong chat body.
- Chưa có token-level SQL syntax highlighting.
