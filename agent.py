
# agent.py
import asyncio
import os
import json
import re
from dotenv import load_dotenv
from textwrap import dedent
from typing import Optional, List

import pandas as pd
from data_manager import data_manager

# Try to load from .env first, then fall back to Streamlit secrets
load_dotenv()

# Helper function to get config values
def get_config(key: str, default: str = "") -> str:
    """Get config from environment or Streamlit secrets"""
    # First try environment variables
    value = os.getenv(key)
    if value:
        return value
    
    # Then try Streamlit secrets
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except:
        pass
    
    return default

# --------------------------------------------------------------------------- #
# LLM Wrapper
# --------------------------------------------------------------------------- #
class LLMModel:
    def __init__(self, model_id: str, api_key: str):
        self.model_id = model_id
        self.api_key = api_key
        try:
            from groq import Groq
            self.client = Groq(api_key=api_key)
        except ImportError:
            print("Groq not installed – using mock.")
            self.client = None

    async def generate_response(
        self, messages: list, temperature: float = 0.0, max_tokens: int = 300
    ) -> str:
        if self.client is None:
            return "Mock response (install groq)."

        try:
            resp = self.client.chat.completions.create(
                model=self.model_id,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content
        except Exception as e:
            return f"LLM error: {e}"


def get_model(model_id: str, api_key: str) -> LLMModel:
    return LLMModel(model_id, api_key)


# --------------------------------------------------------------------------- #
# System Prompt
# --------------------------------------------------------------------------- #
INSTRUCTIONS = dedent(
    """\
    You are a helpful data analyst. Provide clear, actionable insights about the data.

    Guidelines:
    - When user asks to "select", "show", "list", or "get" specific data, ALWAYS display the actual results
    - For list requests, show ALL results (or state "showing X of Y results")
    - For analysis requests, provide insights with key findings
    - Use bullet points for clarity
    - Highlight important patterns
    - Be direct and actionable
    - Keep responses concise but complete

    CRITICAL: If the query returns specific items (titles, names, categories, etc.), LIST THEM ALL in your response.
    """
)

# --------------------------------------------------------------------------- #
# Main Agent
# --------------------------------------------------------------------------- #
async def run_agent(
    message: str, model_id: Optional[str] = None, dataset_name: Optional[str] = None
) -> dict:
    try:
        model = get_model(
            model_id or get_config("MODEL_ID", "llama-3.3-70b-versatile"), 
            get_config("MODEL_API_KEY")
        )

        info = data_manager.get_dataset_info(dataset_name)
        if "error" in info:
            return {
                "content": "Please upload a dataset first.",
                "metadata": {"error": "No dataset"},
            }

        df = data_manager.datasets[info["name"]]["dataframe"]

        # 1. Generate SQL with LLM
        query = await generate_sql_with_llm(message, info, df, model)

        # 2. Safety net
        if not is_safe_sql(query):
            query = "SELECT * FROM current_data LIMIT 20"

        # 3. Execute
        results = data_manager.query_data(query, dataset_name)

        # Check for errors in results
        if results and isinstance(results[0], dict) and "error" in results[0]:
            error_msg = results[0]["error"]
            # Try to auto-fix common issues
            query = await retry_sql_generation(message, info, df, model, error_msg)
            results = data_manager.query_data(query, dataset_name)
            
            # If still failing, return error with helpful message
            if results and isinstance(results[0], dict) and "error" in results[0]:
                return {
                    "content": f"I couldn't analyze that data. Error: {results[0]['error']}\n\nTry rephrasing your question or ask about specific columns.",
                    "metadata": {
                        "query_used": query,
                        "error": results[0]["error"],
                        "dataset": info["name"]
                    }
                }

        # 4. Build context for final answer
        context = f"""
        Dataset: {info['name']}
        Columns: {', '.join(info['columns'][:6])}
        Results: {len(results)} rows
        Question: {message}
        SQL: {query}
        """

        messages = [
            {"role": "system", "content": INSTRUCTIONS},
            {
                "role": "user",
                "content": f"""{context}

                First 10 rows:
                {json.dumps(results[:10], indent=2)}

                Summarize in <150 words:
                - Main insight
                - Key findings
                - Recommendations
                """,
            },
        ]

        answer = await model.generate_response(messages, max_tokens=300)

        return {
            "content": answer,
            "metadata": {
                "query_used": query,
                "results_count": len(results),
                "dataset": info["name"],
                "columns_analyzed": extract_columns_from_query(query),
            },
        }

    except Exception as e:
        return {"content": f"Error: {e}", "metadata": {"error": str(e)}}


# --------------------------------------------------------------------------- #
# Improved SQL Generator
# --------------------------------------------------------------------------- #
async def generate_sql_with_llm(
    question: str, dataset_info: dict, df: pd.DataFrame, model: LLMModel
) -> str:
    """Ask the LLM to write a single, safe SQL query."""

    # Build schema with proper quoting
    schema_lines = []
    for col in dataset_info["columns"]:
        dtype = str(df[col].dtype)
        # Show example values for context
        sample = df[col].dropna().head(3).tolist()
        sample_str = f" (e.g., {', '.join(map(str, sample[:2]))})" if sample else ""
        schema_lines.append(f"- \"{col}\" ({dtype}){sample_str}")

    schema = "\n".join(schema_lines)

    prompt = f"""
You are a SQL expert. Write ONE valid DuckDB/SQLite query for this question.

Table: current_data
Columns (use EXACT names with double quotes):
{schema}

Question: {question}

CRITICAL RULES:
1. ALWAYS use double quotes around column names: "Column Name"
2. Use aggregation for summaries: AVG(), SUM(), COUNT(), etc.
3. Use GROUP BY when breaking down by categories
4. Use ORDER BY for rankings (add DESC for highest first)
5. LIMIT results to 20 or less
6. Filter out NULL values in WHERE clause when appropriate
7. For "what", "which", "show me" questions: use SELECT DISTINCT or GROUP BY

Example patterns:
- "which categories": SELECT DISTINCT "Category" FROM current_data
- "average by group": SELECT "Group", AVG("Value") as avg_value FROM current_data GROUP BY "Group"
- "top 10": SELECT * FROM current_data ORDER BY "Score" DESC LIMIT 10

Return ONLY the SQL query (no markdown, no explanation, no backticks).

SQL:
"""

    try:
        raw = await model.generate_response(
            [{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=250,
        )

        # Clean up
        sql = raw.strip()
        sql = re.sub(r'^```sql\s*', '', sql, flags=re.I)
        sql = re.sub(r'^```\s*', '', sql, flags=re.I)
        sql = re.sub(r'\s*```$', '', sql)
        sql = sql.strip()

        # Validate
        if not sql.upper().startswith("SELECT"):
            raise ValueError("Not a SELECT query")

        return sql
    except Exception as e:
        print(f"SQL gen failed: {e}")
        return 'SELECT * FROM current_data LIMIT 20'


async def retry_sql_generation(
    question: str, dataset_info: dict, df: pd.DataFrame, model: LLMModel, error: str
) -> str:
    """Retry SQL generation with error context"""
    
    schema_lines = []
    for col in dataset_info["columns"]:
        dtype = str(df[col].dtype)
        schema_lines.append(f"- \"{col}\" ({dtype})")
    schema = "\n".join(schema_lines)

    prompt = f"""
The previous SQL query failed with this error:
{error}

Table: current_data
Columns (use double quotes):
{schema}

Question: {question}

Write a corrected SQL query. Remember:
1. Use double quotes: "Column Name"
2. Use exact column names from schema
3. Keep it simple if the question is ambiguous

Return ONLY the SQL:
"""

    try:
        raw = await model.generate_response(
            [{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=200,
        )
        sql = raw.strip()
        sql = re.sub(r'^```sql\s*', '', sql, flags=re.I)
        sql = re.sub(r'^```\s*', '', sql, flags=re.I)
        sql = re.sub(r'\s*```$', '', sql)
        return sql.strip()
    except:
        return 'SELECT * FROM current_data LIMIT 20'


# --------------------------------------------------------------------------- #
# Safety & Helpers
# --------------------------------------------------------------------------- #
def is_safe_sql(query: str) -> bool:
    banned = [
        "DROP", "DELETE", "INSERT", "UPDATE", "CREATE", "ALTER",
        "TRUNCATE", "EXEC", "EXECUTE", ";--", "/*", "*/"
    ]
    up = query.upper()
    return not any(b in up for b in banned)


def extract_columns_from_query(query: str) -> List[str]:
    """Return list of column names from query"""
    if "SELECT" not in query.upper():
        return []
    
    try:
        select_part = query.split("FROM")[0]
        select_part = re.sub(r'SELECT\s+', '', select_part, flags=re.I).strip()
        
        if "*" in select_part:
            return ["*"]
        
        # Extract column names (handle quotes and aliases)
        cols = []
        for part in select_part.split(","):
            # Remove AS aliases
            part = re.split(r'\s+AS\s+', part, flags=re.I)[0]
            # Extract column name from quotes or bare name
            match = re.search(r'"([^"]+)"|`([^`]+)`|(\w+)', part)
            if match:
                col = match.group(1) or match.group(2) or match.group(3)
                if col and col.upper() not in ['SELECT', 'DISTINCT', 'COUNT', 'AVG', 'SUM', 'MAX', 'MIN']:
                    cols.append(col)
        return cols
    except:
        return []


# --------------------------------------------------------------------------- #
# Demo
# --------------------------------------------------------------------------- #
async def main():
    resp = await run_agent("which education level earns most salary", dataset_name="gw")
    print(resp["content"])


if __name__ == "__main__":
    asyncio.run(main())