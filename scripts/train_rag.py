"""Train Vanna RAG with Golden Queries"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.rag import get_vanna_instance


def train():
    vn = get_vanna_instance()

    # Connect to business DB
    vn.connect_to_postgres(
        host=os.getenv("DB_HOST", "postgres-data"),
        dbname=os.getenv("DB_NAME", "business_db"),
        user=os.getenv("DB_USER", "analyst"),
        password=os.getenv("DB_PASSWORD", "analyst123"),
        port=int(os.getenv("DB_PORT", 5432)),
    )

    # 1. Auto-train with schema
    print("📋 Training with schema...")
    df = vn.run_sql("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
    """)
    plan = vn.get_training_plan_generic(df)
    vn.train(plan=plan)
    print(f"   ✅ Schema trained")

    # 2. Train with golden queries
    print("🏆 Training with golden queries...")
    golden_path = os.path.join(os.path.dirname(__file__), "golden_queries.json")
    with open(golden_path, "r", encoding="utf-8") as f:
        golden = json.load(f)

    for i, item in enumerate(golden):
        vn.train(question=item["question"], sql=item["sql"])
        print(f"   [{i+1}/{len(golden)}] {item['question'][:60]}...")

    print(f"✅ Done! Trained {len(golden)} golden queries.")


if __name__ == "__main__":
    train()
