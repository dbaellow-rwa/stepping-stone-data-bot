from openai import OpenAI
import pandas as pd
import re
from typing import List, Dict

# Import prompts and table summaries from your data_prompts module
from utils.data_prompts import TABLE_SUMMARIES, get_table_prompts, GENERAL_SQL_GUIDELINES

# ──────────────────────────────────────────────────────────────────────────────
# Table name extraction
# ──────────────────────────────────────────────────────────────────────────────
def extract_table_names(text: str) -> List[str]:
    """
    Extract potential BigQuery table names (e.g., 'fct_...') from text,
    and filter them against TABLE_SUMMARIES keys.
    """
    candidates = re.findall(r"\b[a-z0-9_]*fct_[a-z0-9_]+\b", text) + re.findall(r"\bfct_[a-z0-9_]+\b", text)
    return list(set(candidates) & set(TABLE_SUMMARIES.keys()))

# ──────────────────────────────────────────────────────────────────────────────
# SQL generation (2-step): table selection → SQL creation
# ──────────────────────────────────────────────────────────────────────────────
def generate_sql_from_question_modular(question: str, openai_key: str) -> str:
    """
    Generates a BigQuery SQL query for a grocery retail client in two steps:
      1) Select the most relevant table(s)
      2) Generate SQL using selected table prompts + general guidelines
    """
    client = OpenAI(api_key=openai_key)

    # Step 1: choose relevant tables
    table_selection_prompt = f"""
The user asked: "{question}"

You are selecting tables for a grocery retail analytics question.
Available BigQuery tables (summaries):
{chr(10).join([f"- {tbl}: {desc}" for tbl, desc in TABLE_SUMMARIES.items()])}

Return 1 (ideally) or 2 (if necessary) table names ONLY, comma-separated.
Guidelines:
- Choose the table that directly answers the question:
  • Sales, revenue, units, baskets, promos, channels, categories → fct_store_sales
  • Inventory levels, days of supply, stockouts/low stock, aging inventory → fct_inventory_daily
- If two tables are genuinely needed (e.g., sales + current inventory), list both.
- Output should be only table names, e.g.: fct_store_sales OR fct_store_sales, fct_inventory_daily
    """.strip()

    selection_response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": table_selection_prompt}],
        temperature=0.0,
    )
    selected_raw = selection_response.choices[0].message.content
    selected_tables = extract_table_names(selected_raw)

    # Fallback: if nothing parsed, include all known tables (keeps context robust)
    if not selected_tables:
        selected_tables = list(TABLE_SUMMARIES.keys())

    # Step 2: load prompts for those tables
    table_prompts = get_table_prompts()
    selected_prompts = [table_prompts[t] for t in selected_tables if t in table_prompts]
    table_context = "\n\n".join(selected_prompts)

    # Step 3: final SQL prompt
    final_prompt = f"""
You are a SQL assistant for a grocery retail data warehouse in BigQuery.
Use Standard SQL compatible with Google BigQuery.
- Prefer fully-qualified table names (project.dataset.table) if provided in the context.
- Use SAFE_* functions when dividing/casting.
- Use DATE_TRUNC/DATE_SUB/DATE_DIFF for date logic.
- QUALIFY may be used with window functions.

{GENERAL_SQL_GUIDELINES()}
{table_context}

User question: {question}

TASK:
Return ONLY a valid BigQuery SQL query (no explanations, no markdown).
    """.strip()

    sql_response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": final_prompt}],
        temperature=0.0,
    )

    sql = sql_response.choices[0].message.content.strip()
    if "```sql" in sql:
        sql = sql.split("```sql")[-1].split("```")[0].strip()
    elif "```" in sql:
        sql = sql.split("```")[-1].split("```")[0].strip()
    return sql

# ──────────────────────────────────────────────────────────────────────────────
# Result summarization helpers
# ──────────────────────────────────────────────────────────────────────────────
def row_to_sentence(row: pd.Series) -> str:
    """Render a DataFrame row as a readable sentence."""
    return ". ".join([f"{col}: {row[col]}" for col in row.index]) + "."

def summarize_results(
    df: pd.DataFrame,
    openai_key: str,
    question: str,
    conversational_history: list = None,
    generated_sql: str = ""
) -> str:
    """
    Summarize the DataFrame for a grocery audience in 1–3 sentences.
    Emphasize SKU/department/store, dates, revenue/units, and inventory risk.
    """
    client = OpenAI(api_key=openai_key)

    rows_as_sentences = "\n".join(row_to_sentence(row) for _, row in df.iterrows())

    history_context = ""
    if conversational_history:
        for q_prev, a_prev, df_result, sql_prev in conversational_history:
            history_context += f"Previous user query: '{q_prev}'\n"
            history_context += f"Previous assistant answer: '{a_prev}'\n"
            history_context += "---\n"

    # Grocery-specific summary formatting rules
    prompt = f"""
You are an AI assistant specializing in grocery retail analytics summaries.

{history_context if history_context else ""}
Current user question: "{question}"

Here is the SQL that produced the results:
```sql
{generated_sql}
Here are the query results as sentences (row-wise):
{rows_as_sentences}

🧠 CONTEXT RULES

If prior conversation applied filters (date ranges, stores, channels, departments, SKUs), carry them forward unless the new question overrides them.

Do NOT invent filters—only carry forward those clearly used earlier.

📝 OUTPUT FORMAT (Markdown):
ANSWER: A 1–3 sentence plain, analytical summary focused on business takeaways (e.g., revenue, units, AOV, promo share, days of supply, stockouts/low stock).

LOGIC USED: 1–2 sentences explaining (at a high level) how the SQL answered the question (filters, groupings, date periods, metrics).

Bold the following when they appear:

SKU names or product descriptors (e.g., sku_name, brand_name)

Store identifiers/names (e.g., store_id, store_name)

Department/category names (e.g., dept_name, category_name)

Explicit dates or periods (e.g., 2025-09-01 to 2025-09-07, yesterday)

Key metrics and figures (e.g., revenue, units, AOV, promo_share, days_of_supply)

Keep it concise and strictly follow the requested format.
""".strip()
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )
    return response.choices[0].message.content.strip()