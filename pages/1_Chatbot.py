import os
import sys
import time
import uuid
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Chatbot",
    page_icon="🤖",
    initial_sidebar_state="expanded",
    layout="wide",
)
st.title("🤖 Data Chatbot")

import altair as alt
import datetime
import google.cloud.bigquery as bigquery

# Make project root importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ──────────────────────────────────────────────────────────────────────────────
# Generic imports (match the generic modules we created)
# ──────────────────────────────────────────────────────────────────────────────
from config.app_config import CONFIG  # exposes computed BQ log FQNs + env
from utils.bq_utils import (
    AuthConfig,
    load_credentials,
    make_bq_client,
    run_bigquery,
    extract_table_schema,
)
from utils.llm_utils import generate_sql_from_question_modular, summarize_results
from utils.streamlit_utils import (
    log_vote_to_bq,
    log_chatbot_question_to_bq,
    log_error_to_bq,
    log_zero_result_to_bq,
    get_oauth,
    init_cookies_and_restore_user,
)
from utils.security_utils import is_safe_sql
from utils.about_the_chatbot import render_about

# Session cookies / OAuth (keep your implementations)
cookies = init_cookies_and_restore_user()
oauth2, redirect_uri = get_oauth()
render_about()

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _log_error(bq_client, question_text, sql, err, attempt):
    log_error_to_bq(
        bq_client,
        CONFIG.BQ_CHATBOT_ERROR_LOG,
        question_text,
        sql,
        err,
        attempt,
    )

def _log_zero(bq_client, question_text, sql, attempt):
    log_zero_result_to_bq(
        bq_client,
        CONFIG.BQ_CHATBOT_ZERO_RESULT_LOG,
        question_text,
        sql,
        attempt,
    )

def _log_question(bq_client, question_text, sql, summary, is_follow_up, previous_question, context_history):
    log_chatbot_question_to_bq(
        bq_client,
        CONFIG.BQ_CHATBOT_QUESTION_LOG,
        question_text,
        sql,
        summary,
        is_follow_up=is_follow_up,
        previous_question=previous_question,
        context_history=context_history,
    )

# ──────────────────────────────────────────────────────────────────────────────
# Core query processing
# ──────────────────────────────────────────────────────────────────────────────
def process_question(
    question_text: str,
    is_follow_up: bool,
    bq_client: bigquery.Client,
    openai_key: str,
    # Grocery-style optional filters (purely contextual for the LLM)
    store_filter: str,
    department_filter: str,
    channel_filter: str,
    date_range: tuple[datetime.date | None, datetime.date | None],
):
    """
    Generates SQL, runs BigQuery, summarizes results, logs analytics, and updates state.
    Handles multi-turn context automatically.
    """
    st.session_state.query_attempts_count = 0
    start_time = time.time()

    # Build filter context for the LLM (these are *contextual hints*, not enforced here)
    filters_context = ""
    if store_filter:
        filters_context += f"\n- store: {store_filter}"
    if department_filter:
        filters_context += f"\n- department: {department_filter}"
    if channel_filter:
        filters_context += f"\n- channel: {channel_filter}"
    if date_range and (date_range[0] or date_range[1]):
        dr_from = date_range[0].isoformat() if date_range[0] else "unspecified"
        dr_to = date_range[1].isoformat() if date_range[1] else "unspecified"
        filters_context += f"\n- date_range: {dr_from} → {dr_to}"

    # Build conversational history snippet for the LLM (last N turns)
    llm_base_context = ""
    max_previous_qa_pairs_for_llm = 2
    conversation_context_parts = []
    if is_follow_up and st.session_state.history:
        start_index = max(0, len(st.session_state.history) - max_previous_qa_pairs_for_llm)
        context_history_slice = st.session_state.history[start_index:]

        for q_prev, a_prev, df_result, sql_prev in context_history_slice:
            conversation_context_parts.append(f"Previous user query: '{q_prev}'")
            conversation_context_parts.append(f"Assistant's previous answer: '{a_prev}'")
            conversation_context_parts.append(f"Assistant's previous SQL: ```sql\n{sql_prev}\n```")
            if isinstance(df_result, pd.DataFrame) and not df_result.empty:
                preview_rows = df_result.head(5)
                rows_as_sentences = "\n".join(
                    ", ".join(f"{col}: {row[col]}" for col in preview_rows.columns)
                    for _, row in preview_rows.iterrows()
                )
                conversation_context_parts.append("Previous results (structured):\n" + rows_as_sentences)

    conversation_context_parts.append("---")
    if conversation_context_parts:
        llm_base_context += "\n".join(conversation_context_parts) + "\n"

    llm_base_context += f"The user's CURRENT question: '{question_text}'\n\n"
    llm_base_context += "Please generate SQL to answer this question. "
    if is_follow_up:
        llm_base_context += (
            "Consider the preceding conversation history to provide a contextually relevant answer. "
            "Do not repeat information already provided by previous SQL/answers unless specifically asked. "
        )
    llm_base_context += f"[Contextual Filters Applied]{filters_context if filters_context else ' None'}\n\n"

    max_attempts = 5
    error_history: list[str] = []
    zero_result_history: list[str] = []
    sql = ""
    summary = ""
    df = pd.DataFrame()

    while st.session_state.query_attempts_count < max_attempts:
        st.session_state.query_attempts_count += 1
        current_context_for_llm = llm_base_context

        if error_history or zero_result_history:
            if zero_result_history:
                current_context_for_llm += (
                    f"\n\n[NOTE] Previous attempt(s) returned 0 results:\n" + "\n".join(zero_result_history) + "\n\n"
                )
            if error_history:
                current_context_for_llm += (
                    f"[ERROR LOG] Previous attempt(s) returned BigQuery errors:\n" + "\n".join(error_history) + "\n\n"
                )
            current_context_for_llm += (
                "Please revise the SQL to avoid these issues. "
                "Do not use columns or aliases not listed in the 'Important columns' sections of the prompts, "
                "and ensure joins and filters are valid."
            )

        try:
            sql = generate_sql_from_question_modular(current_context_for_llm, openai_key)

            if not is_safe_sql(sql):
                error_str = "Unsafe SQL detected. Execution blocked."
                st.error(f"🚫 {error_str}")
                _log_error(bq_client, question_text, sql, error_str, st.session_state.query_attempts_count)
                summary = (
                    f"🚫 **Query blocked for safety**\n\n"
                    f"**Your question:** {question_text}\n\n"
                    f"**Reason:** Unsafe SQL detected."
                )
                df = pd.DataFrame()
                break

            df = run_bigquery(bq_client, sql)

            if df.empty:
                zero_result_history.append(f"[Attempt {st.session_state.query_attempts_count}] {sql}")
                _log_zero(bq_client, question_text, sql, st.session_state.query_attempts_count)
                st.warning(f"Attempt {st.session_state.query_attempts_count} returned no results. Retrying...")
                if st.session_state.query_attempts_count == max_attempts:
                    summary = (
                        f"### ⚠️ No results found for your question after {max_attempts} attempts:\n"
                        f"> **{question_text}**\n\n"
                        f"Try:\n"
                        f"- Relaxing filters like store, department, channel, or date range\n"
                    )
                    break
                continue

            # Success
            break

        except Exception as bq_error:
            error_str = str(bq_error)
            error_history.append(f"[Attempt {st.session_state.query_attempts_count}]\nSQL:\n{sql}\nError:\n{error_str}")
            st.warning(f"Attempt {st.session_state.query_attempts_count} failed: {error_str}. Retrying...")
            _log_error(bq_client, question_text, sql, error_str, st.session_state.query_attempts_count)

            if st.session_state.query_attempts_count == max_attempts:
                summary = (
                    f"❌ **Query failed after {max_attempts} attempts.**\n\n"
                    f"**Your question:** {question_text}\n\n"
                    f"**Error details:**\n" + "\n".join(error_history)
                )
                df = pd.DataFrame()
                break
            continue

    # Final summary
    if not summary:
        if df.empty:
            summary = (
                f"### ⚠️ No results found for your question:\n"
                f"> **{question_text}**\n\n"
                f"Try:\n"
                f"- Relaxing filters like store, department, channel, or date range\n"
            )
        else:
            summary = summarize_results(
                df,
                openai_key,
                question_text,
                conversational_history=st.session_state.history[-2:],
                generated_sql=sql,
            )

    # Update session state
    st.session_state.last_duration_seconds = round(time.time() - start_time)
    st.session_state.history.append((question_text, summary, df.copy(), sql))
    st.session_state.last_question = question_text
    st.session_state.last_summary = summary
    st.session_state.last_df = df
    st.session_state.last_sql = sql
    st.session_state.last_question_was_follow_up = is_follow_up

    # Log interaction
    if "Query blocked for safety" not in summary:
        prev_q = st.session_state.history[-2][0] if is_follow_up and len(st.session_state.history) > 1 else None
        context_for_log = "\n".join(conversation_context_parts) if conversation_context_parts else ""
        _log_question(
            bq_client,
            question_text,
            sql,
            summary,
            is_follow_up,
            prev_q,
            context_for_log,
        )

    # Reset follow-up UI controls
    st.session_state.show_follow_up_input = False
    st.session_state.follow_up_question_text = ""
    st.session_state.example_question = ""
    st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    if "user" not in st.session_state or st.session_state.user is None:
        st.warning("🔒 Please log in on the home page first.")
        st.stop()

    st.markdown("Ask a question about your data.")

    # Load credentials using our generic AuthConfig
    cfg = AuthConfig(
        mode=os.environ.get("BQ_AUTH_MODE", "adc"),
        gcp_sa_secret_id=os.environ.get("BQ_SA_SECRET_ID"),
        gcp_sa_secret_project=os.environ.get("BQ_SA_SECRET_PROJECT"),
        openai_secret_id=os.environ.get("OPENAI_SECRET_ID"),
        openai_secret_project=os.environ.get("OPENAI_SECRET_PROJECT"),
        project_id=os.environ.get("GOOGLE_CLOUD_PROJECT"),
    )
    loaded = load_credentials(cfg)
    if not loaded.openai_api_key:
        st.error("Missing OpenAI API key. Please check your credentials.")
        return

    bq_client = make_bq_client(loaded)

    # ───────────────────────────────
    # Session State Initialization
    # ───────────────────────────────
    if "schema" not in st.session_state:
        # Try to extract a known table schema (best effort; harmless if not found)
        try:
            # Example: if you registered prompts for fct_store_sales, show it here:
            st.session_state.schema = extract_table_schema(bq_client, "fct_store_sales")
        except Exception:
            st.session_state.schema = {}
    if "history" not in st.session_state: st.session_state.history = []
    if "votes" not in st.session_state: st.session_state.votes = []
    if "example_question" not in st.session_state: st.session_state.example_question = ""
    if "last_question" not in st.session_state: st.session_state.last_question = ""
    if "last_summary" not in st.session_state: st.session_state.last_summary = ""
    if "last_df" not in st.session_state: st.session_state.last_df = pd.DataFrame()
    if "query_attempts_count" not in st.session_state: st.session_state.query_attempts_count = 0
    if "last_duration_seconds" not in st.session_state: st.session_state.last_duration_seconds = 0
    if "last_sql" not in st.session_state: st.session_state.last_sql = ""
    if "show_follow_up_input" not in st.session_state: st.session_state.show_follow_up_input = False
    if "follow_up_question_text" not in st.session_state: st.session_state.follow_up_question_text = ""
    if "last_question_was_follow_up" not in st.session_state: st.session_state.last_question_was_follow_up = False

    # ───────────────────────────────
    # Sidebar Filters (generic grocery flavor)
    # ───────────────────────────────
    with st.sidebar:
        with st.expander("⚙️ Optional Filters"):
            store_filter = st.text_input("Filter by store (ID or name)", value="")
            department_filter = st.text_input("Filter by department/category", value="")
            channel_filter = st.selectbox("Channel", ["", "in_store", "curbside", "delivery", "ecomm"])
            date_from = st.date_input("From date", value=None)
            date_to = st.date_input("To date", value=None)

        with st.expander("💡 Try an Example"):
            st.subheader("Quick Example Questions")
            example_questions = {
                "🏪 Top 10 SKUs by revenue last 7 days": "Show the top 10 SKUs by revenue in the last 7 days.",
                "📦 Items at stockout risk today": "Which items are at stockout risk today across all stores?",
                "🛒 AOV by channel last month": "What was the average order value by channel last month?",
                "🥑 Produce revenue by store yesterday": "Produce department revenue by store for yesterday.",
            }
            for button_text, q_text in example_questions.items():
                if st.button(button_text, key=f"ex_{hash(q_text)}"):
                    st.session_state.example_question = q_text
                    st.session_state.show_follow_up_input = False
                    st.session_state.last_question = ""
                    st.session_state.follow_up_question_text = ""
                    st.rerun()

    # ────────────────────────────────────────────────────────────────────────
    # Input Area (initial vs follow-up)
    # ────────────────────────────────────────────────────────────────────────
    if not st.session_state.show_follow_up_input:
        question_initial = st.text_area(
            "Ask your question",
            value=st.session_state.example_question,
            height=150,
            key="main_question_input",
        )

        if st.button("Submit Initial Question", key="submit_initial_button"):
            if question_initial:
                st.session_state.question_id = str(uuid.uuid4())
                with st.spinner("Processing initial question..."):
                    process_question(
                        question_initial,
                        False,
                        bq_client,
                        loaded.openai_api_key,
                        store_filter,
                        department_filter,
                        channel_filter,
                        (date_from, date_to),
                    )
            else:
                st.warning("Please enter a question.")
    else:
        st.markdown("---")
        st.subheader("💬 Ask a Follow-up Question")
        st.markdown(f"Regarding: _'{st.session_state.last_question}'_")
        follow_up_question = st.text_area(
            "Your follow-up:",
            value=st.session_state.follow_up_question_text,
            height=100,
            key="follow_up_question_input_display",
        )

        if st.button("Submit Follow-up Question", key="submit_follow_up_button"):
            if follow_up_question:
                st.session_state.question_id = str(uuid.uuid4())
                with st.spinner("Processing follow-up question..."):
                    process_question(
                        follow_up_question,
                        True,
                        bq_client,
                        loaded.openai_api_key,
                        store_filter,
                        department_filter,
                        channel_filter,
                        (date_from, date_to),
                    )
            else:
                st.warning("Please enter a follow-up question.")

    # ────────────────────────────────────────────────────────────────────────
    # Results & Interactions
    # ────────────────────────────────────────────────────────────────────────
    if not st.session_state.last_question and not st.session_state.show_follow_up_input:
        st.info("Ask a question to begin.")
        return

    tab1, tab2, tab3, tab4 = st.tabs(["🧠 Answer", "🧾 SQL", "📊 Results", "📈 Chart"])

    with tab1:
        if not st.session_state.history:
            st.info("No questions asked yet.")
        else:
            st.markdown("### 🧠 Current Question & Answer")
            st.markdown(f"**QUESTION:** {st.session_state.last_question}")
            st.write(st.session_state.last_summary)

            attempts = st.session_state.query_attempts_count
            duration = st.session_state.last_duration_seconds
            st.caption(f"🕒 Answer generated in {attempts} attempt{'s' if attempts > 1 else ''} and {duration} seconds.")

            if not st.session_state.last_df.empty and len(st.session_state.last_df) > 7:
                st.warning(
                    f"Displaying {len(st.session_state.last_df)} rows. "
                    f"This table is large; consider refining your question."
                )
            st.markdown(f"**Rows Returned:** {len(st.session_state.last_df)}")

            if st.session_state.last_question_was_follow_up and len(st.session_state.history) > 1:
                st.markdown("### Previous Turn (Context)")
                prev_q, prev_a, _, _ = st.session_state.history[-2]
                st.markdown(f"**Q:** {prev_q}")
                st.markdown(f"**A:** {prev_a}")
                st.markdown("---")

            if st.session_state.last_question and not st.session_state.show_follow_up_input:
                if st.button("💬 Ask Follow-up Question", key="activate_follow_up_button"):
                    st.session_state.show_follow_up_input = True
                    st.session_state.follow_up_question_text = ""
                    st.rerun()

            st.markdown("---")
            st.markdown("#### Was this answer helpful?")
            vote_col1, vote_col2 = st.columns(2)
            with vote_col1:
                if st.button("👍 Yes", key="vote_up"):
                    st.session_state.votes.append(("👍", st.session_state.last_question, st.session_state.last_summary))
                    log_vote_to_bq(
                        bq_client,
                        CONFIG.BQ_CHATBOT_VOTE_FEEDBACK,
                        "UP",
                        st.session_state.last_question,
                        st.session_state.last_summary,
                    )
                    st.success("Thanks for your feedback!")
            with vote_col2:
                if st.button("👎 No", key="vote_down"):
                    st.session_state.votes.append(("👎", st.session_state.last_question, st.session_state.last_summary))
                    log_vote_to_bq(
                        bq_client,
                        CONFIG.BQ_CHATBOT_VOTE_FEEDBACK,
                        "DOWN",
                        st.session_state.last_question,
                        st.session_state.last_summary,
                    )
                    st.warning("Thanks for your feedback!")

    with tab2:
        st.markdown("### 🧾 Generated SQL")
        st.code(st.session_state.last_sql or "--", language="sql")

    with tab3:
        st.markdown("### 📊 Results")
        st.dataframe(st.session_state.last_df)

    with tab4:
        # Try to auto-detect a sensible chart: (sku_name vs metric) or (store vs metric)
        df = st.session_state.last_df
        if df.empty:
            st.info("No chartable data.")
        else:
            # Heuristics for columns
            name_cols = [c for c in df.columns if c in ("sku_name", "category_name", "dept_name", "store_name", "store_id")]
            metric_cols = [c for c in df.columns if c in ("revenue", "net_amount", "units", "qty", "days_of_supply", "on_hand_units")]

            if name_cols and metric_cols:
                x_col = name_cols[0]
                y_col = metric_cols[0]
                st.markdown(f"### 📈 Chart: {x_col} vs. {y_col}")
                chart = alt.Chart(df).mark_bar().encode(
                    x=alt.X(f"{x_col}:N", sort="-y", title=x_col.replace("_", " ").title()),
                    y=alt.Y(f"{y_col}:Q", title=y_col.replace("_", " ").title()),
                    tooltip=[x_col, y_col],
                ).properties(height=400)
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No suitable columns detected for a quick chart (looking for name + metric columns).")

    with st.expander("📜 Full Conversation History", expanded=True):
        if not st.session_state.history:
            st.write("No conversation history yet.")
        else:
            for i, (q, a, _, _) in enumerate(reversed(st.session_state.history)):
                st.markdown(f"**Turn {len(st.session_state.history) - i}**")
                st.markdown(f"**Q:** {q}")
                st.markdown(f"**A:** {a}")
                st.markdown("---")

    with st.expander("🗳️ Feedback Log"):
        if not st.session_state.votes:
            st.write("No feedback yet.")
        else:
            for vote, q, a in reversed(st.session_state.votes):
                st.markdown(f"{vote} on **Q:** _{q}_\n> {a[:200]}...")

if __name__ == "__main__":
    main()
