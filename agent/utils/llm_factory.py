"""LLM Factory to instantiate models based on settings."""
import os
from langchain_openai import ChatOpenAI
from langchain_community.llms import Ollama


def get_llm(model_type="small", temperature=0):
    if model_type in ["small", "router"]:
        provider = os.getenv("ROUTER_PROVIDER", "groq").lower()
        model_name = os.getenv("ROUTER_MODEL", "llama3-8b-8192")
        api_key = os.getenv("ROUTER_API_KEY", "")
    else:
        provider = os.getenv("GENERATOR_PROVIDER", "openai").lower()
        model_name = os.getenv("GENERATOR_MODEL", "gpt-4o")
        api_key = os.getenv("GENERATOR_API_KEY", "")

    timeout = float(os.getenv("LLM_TIMEOUT", "20"))
    
    if provider == "groq":
        from langchain_groq import ChatGroq
        if not api_key:
            raise RuntimeError(f"Missing GROQ_API_KEY for Groq provider ({model_type})")
        return ChatGroq(model=model_name, temperature=temperature, api_key=api_key, timeout=timeout, max_retries=1)
        
    elif provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI
        if not api_key:
            raise RuntimeError(f"Missing GEMINI_API_KEY for Gemini provider ({model_type})")
        return ChatGoogleGenerativeAI(model=model_name, temperature=temperature, google_api_key=api_key, timeout=timeout)
        
    elif provider == "ollama":
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        return Ollama(model=model_name, base_url=base_url, temperature=temperature)
        
    else: # openai fallback
        return ChatOpenAI(model=model_name, temperature=temperature, api_key=api_key, timeout=timeout, max_retries=1)
