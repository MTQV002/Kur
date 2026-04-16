"""LLM Factory to instantiate models based on settings."""
import os
from langchain_openai import ChatOpenAI
from langchain_community.llms import Ollama


def get_llm(model_type="small", temperature=0):
    provider = os.getenv("LLM_PROVIDER", "openai").lower()
    timeout = float(os.getenv("LLM_TIMEOUT", "20"))
    
    # Defaults depending on the task "small" (for intent, formatting) or "large" (for SQL Gen)
    if provider == "groq":
        from langchain_groq import ChatGroq
        model_name = os.getenv("LLM_MODEL", "llama-3.1-8b-instant" if model_type == "small" else "llama-3.3-70b-versatile")
        api_key = os.getenv("GROQ_API_KEY") or os.getenv("API_KEY")
        if not api_key:
            raise RuntimeError("Missing GROQ_API_KEY (or API_KEY) for Groq provider")
        return ChatGroq(model=model_name, temperature=temperature, api_key=api_key, timeout=timeout, max_retries=1)
        
    elif provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI
        model_name = os.getenv("LLM_MODEL", "gemini-1.5-flash" if model_type == "small" else "gemini-2.5-pro")
        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("API_KEY")
        if not api_key:
            raise RuntimeError("Missing GEMINI_API_KEY (or API_KEY) for Gemini provider")
        return ChatGoogleGenerativeAI(model=model_name, temperature=temperature, google_api_key=api_key, timeout=timeout)
        
    elif provider == "ollama":
        model_name = os.getenv("OLLAMA_MODEL", "llama3.1:8b" if model_type == "small" else "snowflake-arctic-text2sql-r1:7b")
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        # Ollama does not implement chat model exactly the same way in all langchain versions, using base Ollama for generic
        return Ollama(model=model_name, base_url=base_url, temperature=temperature)
        
    else: # openai fallback
        model_name = os.getenv("LLM_MODEL", "gpt-4o-mini" if model_type == "small" else "gpt-4o")
        api_key = os.getenv("OPENAI_API_KEY") or os.getenv("API_KEY")
        return ChatOpenAI(model=model_name, temperature=temperature, api_key=api_key, timeout=timeout, max_retries=1)

