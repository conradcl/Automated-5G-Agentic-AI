import json
import os
from typing import Any, Dict

from dotenv import load_dotenv
from openai import OpenAI, APIStatusError, APIConnectionError, APITimeoutError


load_dotenv()


def ask_deepseek_about_state(state: Dict[str, Any], user_question: str) -> str:
    api_key = os.getenv("DEEPSEEK_API_KEY")

    if not api_key:
        return (
            "LLM summary unavailable: missing DEEPSEEK_API_KEY. "
            "Add it to your .env file."
        )

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.deepseek.com"
    )

    state_json = json.dumps(state, indent=2)

    system_prompt = """
You are an AI assistant for a 5G/O-RAN testbed.

You will be given structured JSON collected from a live OAI/FlexRIC testbed.
Your job is to explain the current network state clearly and honestly.

Rules:
- Do not invent metrics that are not present.
- If a diagnostics section is present, use it as the primary evidence.
- Distinguish between deterministic tool findings and your own interpretation.
- If something is unhealthy, explain the likely issue based only on the JSON.
- If the system is healthy, say what evidence supports that.
- Recommend safe next actions only.
- Do not claim that an action was performed unless the JSON says it was.
- Keep the answer concise and useful for an engineer.
"""

    user_prompt = f"""
User question:
{user_question}

Live testbed state JSON:
{state_json}
"""

    try:
        response = client.chat.completions.create(
            model="deepseek-v4-flash",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            stream=False
        )

        return response.choices[0].message.content

    except APIStatusError as error:
        if error.status_code == 402:
            return (
                "LLM summary unavailable: DeepSeek returned 402 Insufficient Balance. "
                "The system-state collector still worked, but the LLM call could not run "
                "until the API account has available balance."
            )

        return (
            f"LLM summary unavailable: API status error {error.status_code}. "
            f"Details: {error}"
        )

    except APIConnectionError:
        return (
            "LLM summary unavailable: could not connect to the DeepSeek API. "
            "Check internet access from the VM."
        )

    except APITimeoutError:
        return "LLM summary unavailable: DeepSeek API request timed out."

    except Exception as error:
        return f"LLM summary unavailable: unexpected error: {error}"