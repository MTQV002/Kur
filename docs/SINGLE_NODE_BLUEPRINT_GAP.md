# Cập nhật GAP Analysis: Kur vs SOTA Agentic Text-to-SQL (Apr 2026)

Tài liệu này đánh giá khoảng cách (GAP) giữa kiến trúc hệ thống **Kur AI hiện tại** (đã hoàn thành Phase 6: Dual-Model LangGraph, Polaris, Phoenix) so với các **State-of-the-Art (SOTA)** thực tế trên thị trường như *Databricks Genie, Snowflake Cortex, Uber QueryGPT, và Grab HubbleIQ*. 

Danh sách này được dùng làm nền tảng (Roadmap) để phát triển Kur AI thành một Sản Phẩm Enterprise thực thụ.

---

## 1. Đánh giá Kiến Trúc Hiện Tại (Những gì Kur đã đạt được)

| Đặc Điểm (SOTA)               | Mô hình Kur Hiện Tại                                                                          | Cấp độ Đạt Được   |
| ----------------------------- | --------------------------------------------------------------------------------------------- | ----------------- |
| **Multi-Agent Orchestration** | **Có.** Đã tách Dual-Model: Router (Fast model định tuyến) & Generator (Heavy model - ReAct). | 🟢 Đạt chuẩn SOTA |
| **Tool Calling / Reasoning**  | **Có.** Generator Loop sử dụng `get_database_schema` và `check_sql_syntax` để Self-correct.   | 🟢 Đạt chuẩn SOTA |
| **B2B UI/UX Experience**      | **Có.** Dual-phase execution (Duyệt trước khi lấy Data), Minimalist Inter/Lucide UI.          | 🟢 Đạt chuẩn SOTA |
| **Observability**             | **Có.** Instrumentation sâu bằng LLM Tracing (Arize Phoenix & OpenTelemetry).                 | 🟢 Đạt chuẩn SOTA |
| **Metadata Catalog**          | **Có.** Nối thẳng Apache Polaris (REST Iceberg) qua DuckDB.                                   | 🟢 Đạt chuẩn SOTA |

👉 **Nhận Xét:** Kur đã lột xác hoàn toàn khỏi mức "POC Học thuật" và chạm chân tới kiến trúc của các Tool SaaS B2B tiêu chuẩn thông qua LangGraph Agent, Observability và Dual-Model Orchestrator. 

---

## 2. Phân Tính GAP Đa Chiều (Những gì Kur đang Đói và Thiếu trầm trọng)

Dựa trên nghiên cứu về Cortex, Genie và Paper `AV-SQL`, `OmniSQL`, hệ thống LLM Text-to-SQL **sẽ chết chắc về độ chính xác (Accuracy) nếu không có Context (ngữ cảnh) doanh nghiệp**.

Dưới đây là 5 khoảng trống (Gaps) cốt lõi phải lấp đầy:

### GAP 1: Semantic Layer (Thiếu Hụt Nghĩa Kinh Doanh) - 🔴 CRITICAL
- **Tại sao cần?** Hiện tại LLM chỉ đọc được "Schema Vật Lý" (Tên bảng `ord_fct`, Tên cột `amt`). LLM phải "mò" xem `amt` có phải là Doanh Thu không, dẫn đến **Hallucination cực cao** (Join sai, Group sai). Snowflake Cortex sử dụng cấu trúc `YAML Semantic Model` để rào lại. Databricks có Unity Catalog Data Dictionary.
- **Kur đang thiếu:** Chưa có tầng Model ngữ nghĩa (Dimensions, Measures, Relationships, Synonyms) để tiêm vào System Prompt của Agent. Nền tảng cần hiểu **"Doanh thu" = `SUM(amount)`**.

### GAP 2: Knowledge Store / Verified Queries (Thiếu RAG Golden SQL) - 🔴 CRITICAL
- **Tại sao cần?** Các hệ thống như Uber QueryGPT áp dụng nguyên tắc tìm kiếm các "Câu SQL chuẩn" (Golden SQL) bằng Vector DB, sau đó nạp làm Few-shot example cho LLM. Điều này tăng độ chính xác lên 70-80% do LLM học mót format.
- **Kur đang thiếu:** Kur đang viết SQL dạng "Zero-shot" (tay không bắt giặc). Cần có kho Knowledge Store để Agent Router `RAG` mẫu câu SQL tương tự trước khi Generator bắt đầu viết. 

### GAP 3: Column Pruning Agent (Tràn Bộ Nhớ Context) - 🟡 MAJOR
- **Tại sao cần?** Uber QueryGPT tiết lộ rằng đối với bảng có hàng trăm Cột (Columns), đẩy hết vô Prompt sẽ làm LLM "quá tải" context, bị ngáo và chậm. Do đó cần một bước Filter/Pruner để lược đi các cột không liên quan.
- **Kur đang thiếu:** Hàm `get_database_schema` của Kur đang hút cạn toàn bộ cấu trúc bảng và đẩy hết vào ReAct Tool memory. Càng quy mô lớn Kur sẽ càng chậm và dễ treo. 

### GAP 4: Benchmarking / Evaluation System - 🟡 MAJOR
- **Tại sao cần?** Trong quá trình tinh chỉnh (Prompts, Model mới như Arctic-R1), nếu không có bộ Test tự động (LLM-as-a-judge / Exact Match Evaluation) thì không thể biết phiên bản cập nhật 2.0 có tốt hơn bản 1.0 hay không.
- **Kur đang thiếu:** Bắt buộc phải có một bộ Framework Regression Test ngầm bên dưới (Evaluator Agent) để đo lường Accuracy cho mọi tính năng thêm mới.

### GAP 6: Unified Metadata Federation (Trino as The True Dominator) - ⚪ ARCHITECTURAL SHIFT
- **Tại sao cần?** Hiện tại Apache Polaris chỉ quản lý định dạng Apache Iceberg. Nó không thể giao tiếp trực tiếp để lấy Schema/Data từ Postgres, MySQL như tính năng Lakehouse Federation của Databricks Unity Catalog. Nếu user có DB on-prem (RDMS), Polaris không thể "thống trị" chúng.
- **Kur đang định hướng:** Thay vì ép toàn bộ User phải Ingest data từ Postgres sang S3 Iceberg (rất tốn kém), kiến trúc Kur tương lai phải chuyển sang mô hình: `Kur Agent -> Trino (Công cụ truy vấn phân tán) -> [Polaris Catalog (Iceberg), Postgres Catalog, MySQL Catalog]`. Lúc này, Trino mới là Vị vua Thống Trị, cung cấp metadata của MỌI LOẠI ENGINE thông qua 1 cổng `information_schema` đồng nhất cho LLM đọc.

---

## 3. Kiến Trúc Sắp Tới: Action Plan (Từ Giờ Đến Scale-out)

Để lấp đầy các GAP trên cho Kur, đây là **Backlog tính năng ưu tiên Phát Triển Tiếp:**

#### Q-Term 1: Xóa tan Hallucination bằng Semantic & RAG
1. **Thiết kế Semantic YAML Config Parser:** Cho phép User khai báo file `.yml` (định nghĩa Metric, Join path, Synonym) nằm đệm giữa Polaris và Kur Agent.
2. **Cấy RAG VectorDB (Chroma/Milvus):** Cấu hình tính năng "Tạo Verified Query". Kur Agent sẽ tra cứu Vector Similarity từ các "Câu hỏi kinh điển" để ép LLM trả lời theo định dạng cố định.

#### Q-Term 2: Giải phóng Context Memory
3. **Column Pruner Agent:** Nâng cấp Planner. Thêm một vòng Lặp (Edge Logic): Cắt giảm các cột không cần thiết dựa vào Keyword trước khi đưa cho SQL Generator.
4. **Nâng cấp sang SOTA Model Offline:** Thay thế GPT-4o-mini bằng cài đặt Native `Arctic-Text2SQL-R1` (7B Parameter - RLHF siêu đỉnh của Snowflake) chuyên biệt cho Tool `check_sql_syntax`.

#### Q-Term 3: Trust & Scale
5. **Benchmarking Tool:** Cài đặt Evaluation script dựa theo BIRD Benchmarks. Xác thực tự động.
6. **Multi-turn Clarification:** Nâng cấp Tool Handoff: Nếu User đưa câu hỏi không rõ ràng về Ngữ Nghĩa → Bật tool `ask_user_clarification` (trả về UI cho người dụng yêu cầu Confirm) thay vì đoán mò SQL.
