from __future__ import annotations
import json, os, datetime as dt
from typing import Dict, List
from pydantic import BaseModel, Field
from openai import OpenAI

class Pick(BaseModel):
    symbol: str
    weight: float
    rationale: str

class PolicyResponse(BaseModel):
    asof: str
    picks: List[Pick] = Field(default_factory=list)
    notes: str = ""
    confidence: float = 0.5

SYSTEM_PROMPT = """You are a portfolio construction agent for U.S. equities & ETFs.
Goal: maximize 1-year risk-adjusted returns with low churn and respect constraints.
Hard constraints:
- Long-only, cash only. NO leverage. Max 10 positions. Max single-name weight 20%.
- Only from universe provided (S&P500 & large ETFs; optional R2000 via ETFs).
- Prefer broader trend alignment (intermediate momentum), avoid over-trading.
- Consider trading frictions, market hours, and liquidity. Avoid illiquid names.
- Minimize turnover: prefer small adjustments unless conviction is high.
Return concise rationales.
"""

def choose_portfolio(candidates_json: str, target_positions: int, max_weight: float, model: str = None, memory_context: str = "") -> Dict:
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    model = model or os.getenv("MODEL_NAME", "gpt-5")
    effort = os.getenv("REASONING_EFFORT", "medium")

    schema = {
        "name": "PolicyResponse",
        "schema": {
            "type": "object",
            "properties": {
                "asof": {"type": "string"},
                "picks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "symbol": {"type": "string"},
                            "weight": {"type": "number", "minimum": 0, "maximum": max_weight},
                            "rationale": {"type": "string"}
                        },
                        "required": ["symbol", "weight", "rationale"],
                        "additionalProperties": False
                    },
                    "maxItems": target_positions
                },
                "notes": {"type": "string"},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1}
            },
            "required": ["asof", "picks"]
        },
        "strict": True
    }

    resp = client.responses.create(
        model=model,
        reasoning={"effort": effort},
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": "Memory context (recap of recent episodes):\n" + (memory_context or "None")},
            {"role": "user", "content": "Candidate panel (JSON):\n" + candidates_json},
            {"role": "user", "content": f"Return <= {target_positions} symbols; cap {max_weight:.2f} each; total weight <= 1.0. Favor durable trends; keep turnover low."}
        ],
        response_format={"type":"json_schema", "json_schema": schema},
        temperature=0.2
    )

    try:
        content = resp.output[0].content[0].text
    except Exception:
        content = getattr(resp, "output_text", None) or "{}"

    try:
        parsed = PolicyResponse.model_validate_json(content)
    except Exception as e:
        parsed = PolicyResponse(asof=dt.datetime.utcnow().isoformat(), picks=[], notes=f"ValidationError: {e}", confidence=0.0)

    tw = sum(p.weight for p in parsed.picks)
    if tw > 1.0 and tw > 0:
        for p in parsed.picks:
            p.weight = p.weight / tw
    for p in parsed.picks:
        p.weight = min(max(p.weight, 0.0), max_weight)

    return json.loads(parsed.model_dump_json())
