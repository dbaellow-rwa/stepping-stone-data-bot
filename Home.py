# cd "C:\Users\dusti\OneDrive\Documents\GitHub\trilytxbot"
# streamlit run Home.py
import streamlit as st
st.set_page_config(
    page_title="Home",
    page_icon="https://github.com/dbaellow-rwa/trilytxbot/blob/fe681401e506fd4deccca9fa7c0c751c2cbbf070/assets/logo.png?raw=true",
    initial_sidebar_state="expanded",
    layout="wide")

import json
import requests as pyrequests  # rename to avoid conflict with google.auth.transport.requests
from streamlit_oauth import OAuth2Component
from config.app_config import USE_LOCAL
from utils.streamlit_utils import render_login_block,get_oauth,init_cookies_and_restore_user
cookies = init_cookies_and_restore_user()
oauth2, redirect_uri = get_oauth()

import warnings
if not USE_LOCAL:
    warnings.filterwarnings("ignore", category=DeprecationWarning)
# ───────────────────────────────
# Shared Beta Overview Block
# ───────────────────────────────

# def show_beta_overview():
#     audience = st.radio(
#         "Choose your perspective:",
#         ["Triathlon Community", "Data Scientists", "Sponsors & Media"],
#         horizontal=True
#     )

#     if audience == "Triathlon Community":
#         show_beta_overview_triathletes()
#     elif audience == "Data Scientists":
#         show_beta_overview_data_scientists()
#     else:
#         show_beta_overview_sponsors()


def show_beta_overview_data_scientists():
    st.markdown("""
<div style="max-width: 900px; margin: 0 auto; text-align: left;">

<h1 style="text-align: center;">🤖 Welcome to the Trilytx Beta</h1>

<h3>🧠 What Is Trilytx?</h3>
<p><strong>Trilytx</strong> is an AI assistant trained on triathlon performance data — optimized for natural language querying, race recap generation, and performance analytics across athletes, races, and seasons.</p>
<p>This beta gives data scientists early access to explore how LLMs interact with structured sports data.</p>

<hr/>

<h3>🧪 Why This Beta Matters</h3>
<p>We’re building a <strong>domain-adapted LLM</strong> for triathlon. That means fine-tuning on:</p>
<ul>
  <li>📊 Structured race data (e.g., splits, rankings, scores)</li>
  <li>🧮 SQL queries aligned with real schemas</li>
  <li>🗣️ Natural language questions from coaches, fans, and broadcasters</li>
</ul>
<p>This is your chance to shape how LLMs handle analytical reasoning in niche sports domains.</p>

<hr/>

<h3>📈 What’s New</h3>
<ul>
  <li><strong>Athlete Profiles</strong>: Dynamic views built from segment-level data, PTO scores, and rankings</li>
  <li><strong>LLM Race Recaps</strong>: Full-text summaries generated from structured tables, with customizable prompts</li>
</ul>

<hr/>

<h3>🧬 LLM Evolution: Beta vs Fine-Tuned Model</h3>
<table style="width:100%; border-collapse: collapse;">
<thead>
  <tr>
  <th style="text-align:left; background-color:#fff8dc; padding: 8px;">🟡 Current Beta</th>
  <th style="text-align:left; background-color:#e6ffe6; padding: 8px;">🟢 Post-Fine-Tuning</th>
  </tr>
</thead>
<tbody>
  <tr><td>OpenAI base model (GPT-4)</td><td>Fine-tuned with triathlon-specific tables + QA pairs</td></tr>
  <tr><td>Manual prompt engineering often required</td><td>Understands schema + joins natively</td></tr>
  <tr><td>Relies on retries/table guessing</td><td>Deterministic path: NL → SQL → Result</td></tr>
  <tr><td>Some ambiguity in answers</td><td>Sharper, more contextual responses</td></tr>
  <tr><td>Struggles with nested or comparative logic</td><td>Handles ranking, deltas, and time-windowed analysis</td></tr>
  <tr><td>Recaps are generated via generic LLM</td><td>Recaps tuned on real summaries + tri logic</td></tr>
  <tr><td>Feedback = upvotes/downvotes</td><td>Retraining based on corrections + usage logs</td></tr>
</tbody>
</table>

<hr/>

<h3>🧠 Help Us Train Better Models</h3>
<ol>
  <li>Query with real triathlon questions — simple and complex</li>
  <li>Submit weird edge cases (e.g., “Top 5 swim gainers over 3 years”)</li>
  <li>Use the LLM recap tool and tweak prompt styles</li>
  <li>Vote on answer quality — this is used in future RLHF</li>
</ol>

<hr/>

<h3>🎁 Beta Perks</h3>
<ul>
  <li>Early access to fine-tuned models + embeddings</li>
  <li>Option to join developer Slack + feedback sessions</li>
  <li>Recognition as a Founding Contributor</li>
</ul>

<hr/>

<h3>🔗 Join the Beta</h3>
<p><a href="https://docs.google.com/forms/d/e/1FAIpQLScAA8LmWCd0WUupNBp9QstbAtqkJNXwqkokTlJMb731xovzRA/viewform?usp=dialog" target="_blank"><strong>Click here to sign up</strong></a></p>

<p>Thanks for helping us push the boundaries of domain-specific LLMs in sports analytics.</p>

</div>
""", unsafe_allow_html=True)

def show_beta_overview_triathletes():
    st.markdown("""
<div style="max-width: 900px; margin: 0 auto; text-align: left;">

<h1 style="text-align: center;">🏁 Welcome to the Trilytx Beta</h1>

<h3>🎯 What Is Trilytx?</h3>
<p><strong>Trilytx</strong> is your personal triathlon data assistant — built to answer your questions, track athletes, and create full-length race recaps using real race data.</p>
<p>It’s fast, fun, and full of insight — and you’re helping us shape it as a <strong>Beta Tester</strong>.</p>

<hr/>

<h3>💬 What Can I Ask?</h3>
<ul>
  <li>“How did [athlete name] perform in 2024 races?”</li>
  <li>“Who had the fastest bike split at Ironman Texas?”</li>
  <li>“Compare PTO scores for top women at 70.3 Worlds”</li>
</ul>
<p>Just ask in plain English — no spreadsheets or code required.</p>

<hr/>

<h3>🚨 New Features This Week</h3>
<ul>
  <li><strong>Athlete Profiles</strong>: View race history, segment splits, rankings, and PTO score trends — all in one place</li>
  <li><strong>Race Recaps</strong>: Get a full written summary of any race — dramatic, technical, or playful, based on your style</li>
</ul>

<hr/>

<h3>🛠️ What’s In Beta (and What’s Coming)</h3>
<table style="width:100%; border-collapse: collapse;">
<thead>
  <tr>
  <th style="text-align:left; background-color:#fff8dc; padding: 8px;">🟡 Now</th>
  <th style="text-align:left; background-color:#e6ffe6; padding: 8px;">🟢 Soon</th>
  </tr>
</thead>
<tbody>
  <tr><td>Answers based on general AI</td><td>Answers trained specifically for triathlon</td></tr>
  <tr><td>Some guesswork needed in phrasing</td><td>Understands your questions better over time</td></tr>
  <tr><td>May not catch every nuance</td><td>More precise answers for multi-race and split comparisons</td></tr>
  <tr><td>Basic feedback system</td><td>Smarter over time with your input</td></tr>
  <tr><td>Race recaps via AI + your prompt</td><td>Recaps will “just work” with one click</td></tr>
</tbody>
</table>

<hr/>

<h3>🏆 Help Build the Future of Triathlon Data</h3>
<p>Right now, Trilytx is powered by carefully curated race data from 2020 onward — hand-picked to show what’s possible. But our goal is much bigger.</p>
<p>We’re working to gain full access to the complete datasets — every race, every athlete, every split — so we can:</p>
<ul>
  <li>Answer deeper questions across seasons and distances</li>
  <li>Spot trends, breakthroughs, and rivalries with precision</li>
  <li>Build the most complete triathlon intelligence platform ever</li>
</ul>
<p><strong>You can help us get there:</strong></p>
<ol>
  <li><strong>Ask real questions</strong> — the more demand we show, the stronger the case for full access</li>
  <li><strong>Generate recaps</strong> and explore profiles — see what’s possible with smart tools</li>
  <li><strong>Vote</strong> 👍 or 👎 to improve the answers</li>
  <li><strong>Share Trilytx</strong> with fans, athletes, and teams who want smarter insights</li>
</ol>
<p>With your help, we can build the ultimate triathlon data assistant — one that transforms how we understand the sport.</p>

<hr/>

<h3>🎁 Perks for Beta Testers</h3>
<ul>
  <li>🚀 Early access to new tools and dashboards</li>
  <li>🏅 Founding Beta User recognition</li>
  <li>🎉 Surprise rewards for top users</li>
</ul>

<hr/>

<h3>✍️ Join the Trilytx Beta</h3>
<p><a href="https://docs.google.com/forms/d/e/1FAIpQLScAA8LmWCd0WUupNBp9QstbAtqkJNXwqkokTlJMb731xovzRA/viewform?usp=dialog" target="_blank"><strong>Click here to sign up</strong></a></p>

<p>Thanks for helping us build something amazing for the triathlon world. Let’s level up the way we understand the sport. 🏊‍♂️🚴‍♀️🏃‍♂️</p>

</div>
""", unsafe_allow_html=True)

def show_beta_overview_sponsors():
    st.markdown("""<div style="max-width: 900px; margin: 0 auto; text-align: left;">
<h1 style="text-align: center;">📣 Welcome to the Trilytx Beta</h1>

<h3>💡 What Is Trilytx?</h3>
<p><strong>Trilytx</strong> is an AI-driven triathlon analytics platform that turns raw race data into stories, spotlights, and strategic insights. Whether you're a brand, broadcaster, or media partner, Trilytx makes it easy to track athlete performance, uncover trends, and bring data-driven context to the sport.</p>

<hr/>

<h3>📺 Why This Matters for You</h3>
<ul>
  <li>📊 <strong>Instant Insights</strong>: Need to know who's surging mid-season? Who’s most improved on the bike? Get the data in seconds.</li>
  <li>📝 <strong>AI-Generated Recaps</strong>: Use structured race data to automatically create compelling summaries for web, email, or commentary.</li>
  <li>📈 <strong>Performance Trends</strong>: Visualize PTO score progressions, segment strengths, and comparative head-to-heads over time.</li>
</ul>

<p>As triathlon coverage and sponsorship become more sophisticated, Trilytx helps turn complexity into clarity.</p>

<hr/>

<h3>🆕 What’s New in Beta</h3>
<ul>
  <li><strong>Athlete Profiles</strong>: Media-ready overviews of athlete stats, rankings, race history, and performance narratives</li>
  <li><strong>Custom Race Recaps</strong>: Highlight any angle — drama, strategy, pacing, comeback stories</li>
</ul>

<hr/>

<h3>📊 Current Capabilities vs the Future</h3>
<table style="width:100%; border-collapse: collapse;">
<thead>
  <tr>
  <th style="text-align:left; background-color:#fff8dc; padding: 8px;">🟡 Today</th>
  <th style="text-align:left; background-color:#e6ffe6; padding: 8px;">🟢 Coming Soon</th>
  </tr>
</thead>
<tbody>
  <tr><td>OpenAI-powered insights + summaries</td><td>LLM trained on triathlon-specific storytelling + analytics</td></tr>
  <tr><td>Manual recap tuning (tone, emphasis)</td><td>Auto-recaps tailored to audience type (fan, sponsor, coach)</td></tr>
  <tr><td>Basic athlete comparison tools</td><td>Dynamic side-by-side matchup analysis with visual overlays</td></tr>
  <tr><td>Feedback via thumbs-up/down</td><td>Custom content scoring + recommendations</td></tr>
</tbody>
</table>

<hr/>

<h3>🎯 How You Can Use It</h3>
<ol>
  <li><strong>Generate Recaps</strong> for newsletters, podcasts, and coverage</li>
  <li><strong>Track Sponsored Athletes</strong> across races and seasons</li>
  <li><strong>Request Highlight Packs</strong> focused on brand-aligned metrics (e.g., fastest bike splits)</li>
  <li><strong>Vote on Outputs</strong> — your feedback shapes what Trilytx becomes</li>
</ol>

<hr/>

<h3>🎁 Beta Access Perks</h3>
<ul>
  <li>📊 Media dashboards coming soon</li>
  <li>🎙️ Recap generator for newsletters + scripts</li>
  <li>🏅 Priority access for partner integrations</li>
</ul>

<hr/>

<h3>📬 Join the Beta</h3>
<p><a href="https://docs.google.com/forms/d/e/1FAIpQLScAA8LmWCd0WUupNBp9QstbAtqkJNXwqkokTlJMb731xovzRA/viewform?usp=dialog" target="_blank"><strong>Click here to sign up</strong></a></p>

<p>Help us build the smartest storytelling engine in endurance sports. From spreadsheets to spotlight — Trilytx makes it automatic.</p>
</div>""", unsafe_allow_html=True)

# ───────────────────────────────
# Render Page
# ───────────────────────────────

with st.sidebar:
    render_login_block(oauth2, redirect_uri, cookies)

show_beta_overview_triathletes()