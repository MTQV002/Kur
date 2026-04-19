"""Vanna RAG Instance — Singleton for Golden Query retrieval"""
import os
from dotenv import load_dotenv

load_dotenv()
_vanna_instance = None


def get_vanna_instance():
    """Get or create Vanna instance with ChromaDB backend."""
    global _vanna_instance
    if _vanna_instance is not None:
        return _vanna_instance

    from vanna.legacy.openai import OpenAI_Chat
    from vanna.legacy.chromadb import ChromaDB_VectorStore

    class KurVanna(ChromaDB_VectorStore, OpenAI_Chat):
        def __init__(self, config=None):
            ChromaDB_VectorStore.__init__(self, config=config)
            OpenAI_Chat.__init__(self, config=config)

    _vanna_instance = KurVanna(config={
        "api_key": os.getenv("OPENAI_API_KEY"),
        "model": "gpt-4o-mini",
        "path": os.getenv("CHROMA_PATH", "data/chroma"),
    })

    return _vanna_instance
