import os
os.environ["TRANSFORMERS_NO_TF"]   = "1"
os.environ["TRANSFORMERS_NO_FLAX"] = "1"

import trafilatura
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from transformers import pipeline
import spacy
from groq import Groq
from dotenv import load_dotenv
import json
import re
import time

load_dotenv()

app = FastAPI()

bot_keys = [
    os.getenv("BOT1"), os.getenv("BOT2"), os.getenv("BOT3"),
    os.getenv("BOT4"), os.getenv("BOT5")
]
bot_keys = [k for k in bot_keys if k]  # remove None values
current_bot_index = 0

# Fallback models in order — fastest/highest rate limits first
MODELS = [
    "llama-3.1-8b-instant",      # highest rate limit, very fast
    "gemma2-9b-it",              # good fallback
    "llama3-8b-8192",            # another fast option
    "mixtral-8x7b-32768",        # larger but slower
    "llama-3.3-70b-versatile",   # best quality, lowest rate limit — last resort
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("ALLOWED_ORIGINS", "*")],
    allow_methods=["POST"],
    allow_headers=["Content-Type"],
)

# ─── Models loaded once at startup ───────────────────────────────────────────
sentiment_model = pipeline(
    "sentiment-analysis",
    model="ProsusAI/finbert",
    framework="pt"
)

nlp = spacy.load("en_core_web_sm")

# ─── Prompt builder ───────────────────────────────────────────────────────────
def build_prompt(text: str, doc) -> str:
    spacy_entities = [{"text": ent.text, "type": ent.label_} for ent in doc.ents]
    return f"""
You are an expert financial news analyst. Analyze the news article below and return a structured JSON object.

NEWS ARTICLE:
{text}

SPACY NER HINTS (use these as additional context):
{json.dumps(spacy_entities, indent=2)}

INSTRUCTIONS:
- Analyze the article and classify it into the categories below
- Return ONLY a valid JSON object, no explanation, no markdown, no extra text
- If the article is not relevant to finance or has no effect on markets, return empty arrays for all categories.

ALLOWED VALUES:

regions (pick all that apply):
GLOBAL, NORTH_AMERICA, SOUTH_AMERICA, EUROPE, ASIA_PACIFIC, SOUTH_ASIA, MIDDLE_EAST, AFRICA, AUSTRALIA

sectors (pick all that apply):
TECHNOLOGY, HEALTHCARE, FINANCE, ENERGY, CONSUMER_GOODS, UTILITIES, REAL_ESTATE, AUTOMOBILE, TELECOM, AGRICULTURE, METALS_MINING, AVIATION, INFRASTRUCTURE, INSURANCE

asset_classes (pick all that apply):
EQUITIES, FIXED_INCOME, COMMODITIES, CURRENCIES, DERIVATIVES, CRYPTO, INDEX, REAL_ESTATE

entities_affected: list of actual named entities affected (companies, currencies, commodities, people etc.)

crux: A 2-3 sentence expert explanation of how this news impacts financial markets and investors.
- Explain the cause-and-effect chain clearly. Example: "Fed rate hike → dollar strengthens → FDI outflows from emerging markets → pressure on INR and Indian equities."
- Use plain language a retail investor can understand.
- Include which asset classes, sectors or geographies are most affected and whether the impact is bullish or bearish.
- If the news has no market relevance, set crux to an empty string "".

RETURN THIS EXACT FORMAT:
{{
    "regions":           [],
    "sectors":           [],
    "asset_classes":     [],
    "entities_affected": [],
    "crux":              ""
}}
"""

def parse_llm_response(content: str) -> dict:
    content = re.sub(r"```json|```", "", content).strip()
    return json.loads(content)

def call_groq_with_fallback(prompt: str) -> dict:
    """Try every key x every model until one works."""
    for key in bot_keys:
        for model in MODELS:
            try:
                client = Groq(api_key=key)
                response = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    max_tokens=500,
                )
                result = parse_llm_response(response.choices[0].message.content)
                print(f"[Groq] Success — key ...{key[-4:]} model: {model}")
                return result
            except json.JSONDecodeError:
                print(f"[Groq] {model} returned malformed JSON, skipping")
                continue
            except Exception as e:
                err_str = str(e).lower()
                if "restricted" in err_str or "organization" in err_str:
                    print(f"[Groq] Key ...{key[-4:]} is restricted, trying next key")
                    break  # skip all models for this key, try next key
                elif "rate limit" in err_str or "429" in err_str or "too many" in err_str:
                    print(f"[Groq] Rate limit on {model} key ...{key[-4:]}, trying next model")
                    time.sleep(0.5)
                    continue
                else:
                    print(f"[Groq] Error on {model}: {e}")
                    continue

    print("[Groq] All keys and models failed.")
    return {}

# ─── Route ────────────────────────────────────────────────────────────────────
@app.post("/analyze")
def analyze(article: dict):
    title = article.get("title", "") or ""
    url   = article.get("url",   "") or ""

    # Step 1: Extract full article text from URL
    content = ""
    if url:
        try:
            downloaded = trafilatura.fetch_url(url)
            if downloaded:
                extracted = trafilatura.extract(
                    downloaded,
                    include_comments=False,
                    include_tables=False
                )
                if extracted:
                    content = extracted.strip()
                    print(f"[trafilatura] Extracted {len(content)} chars from {url}")
        except Exception as e:
            print(f"[trafilatura] Error: {e}")

    # Step 2: Fallback if extraction failed
    if not content:
        content = article.get("content", "") or article.get("summary", "") or title
        print(f"[trafilatura] Fallback to summary/title for: {title[:60]}")

    # Step 3: Combined text for models
    text = (title + " " + content).strip()

    # Step 4: FinBERT sentiment analysis
    sentiment = sentiment_model(text[:512])[0]

    # Step 5: spaCy named entity recognition
    doc = nlp(text[:100000])

    # Step 6: Groq — tries all keys x all models automatically
    llm_result = call_groq_with_fallback(build_prompt(text, doc))
    time.sleep(2)  # 2s pause between articles to avoid rate limits


    return {
        "title":             title,
        "content":           content,
        "sentiment":         sentiment,
        "regions":           llm_result.get("regions",           []),
        "sectors":           llm_result.get("sectors",           []),
        "asset_classes":     llm_result.get("asset_classes",     []),
        "entities_affected": llm_result.get("entities_affected", []),
        "crux":              llm_result.get("crux",              ""),
    }