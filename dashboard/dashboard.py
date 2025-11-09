"""
Intelligent Cloud Storage Dashboard
Real-time visualization of tier distribution, movements, and costs
Built with Streamlit
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
import os

# Configuration - works both locally and in Docker
BACKEND_URL = os.getenv("BACKEND_URL", None)
if not BACKEND_URL:
    # Try localhost first (local development), fall back to backend (Docker)
    try:
        import requests
        requests.get("http://localhost:8000/health", timeout=1)
        BACKEND_URL = "http://localhost:8000"
    except:
        BACKEND_URL = "http://backend:8000"

# Branding
TEAM_NAME = "watched_underdog"
BUILDER_NAME = "Adhik Agarwal"
BUILDER_ID = "2022ABPS1364P"

# Page configuration
st.set_page_config(
    page_title="Intelligent Cloud Storage Dashboard",
    page_icon="cloud",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .stApp {
        background-color: #f5f7fb;
        color: #1f2937;
    }
    .main-header {
        font-size: 2.8rem;
        font-weight: 700;
        color: #1f2937;
        text-align: center;
        margin-bottom: 0.25rem;
    }
    .team-badge {
        background: #e0f2fe;
        color: #0f172a;
        padding: 0.6rem 1.4rem;
        border-radius: 999px;
        display: inline-flex;
        gap: 0.4rem;
        font-weight: 600;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }
    .section-divider {
        margin: 1.25rem 0;
        border-bottom: 1px solid #e2e8f0;
    }
    .data-section {
        background: #ffffff;
        border-radius: 1rem;
        padding: 1.5rem;
        border: 1px solid #edf2f7;
        box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
        margin-bottom: 1.5rem;
    }
    .section-title {
        font-size: 1.6rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
        color: #1a202c;
    }
    .section-subtitle {
        color: #64748b;
        font-size: 0.95rem;
        margin-bottom: 1rem;
    }
    .stMetric {
        border: 1px solid #e2e8f0;
        border-radius: 0.75rem;
        padding: 0.5rem;
        background: #fff;
    }
    .badge-tier {
        padding: 0.25rem 0.85rem;
        border-radius: 999px;
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.85rem;
        display: inline-block;
    }
    .badge-hot {
        background: #fdecea;
        color: #c53030;
    }
    .badge-warm {
        background: #fff7e6;
        color: #b7791f;
    }
    .badge-cold {
        background: #e0f2f1;
        color: #0f766e;
    }
    .badge-status {
        padding: 0.25rem 0.85rem;
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.85rem;
        display: inline-block;
    }
    .badge-completed {
        background: #dcfce7;
        color: #15803d;
    }
    .badge-pending, .badge-in_progress {
        background: #fef9c3;
        color: #854d0e;
    }
    .badge-failed {
        background: #fee2e2;
        color: #b91c1c;
    }
    div[data-testid="stSidebar"] {
        background-color: #f8fafc;
        border-right: 1px solid #e2e8f0;
        padding: 1.5rem 1rem;
    }
    .sidebar-heading {
        font-size: 1.15rem;
        font-weight: 600;
        margin-top: 1rem;
        margin-bottom: 0.25rem;
        color: #0f172a;
    }
    .sidebar-caption {
        color: #64748b;
        font-size: 0.85rem;
        margin-bottom: 0.75rem;
    }
    </style>
""", unsafe_allow_html=True)

# Helper functions
def fetch_metrics():
    """Fetch system metrics from backend with graceful error handling"""
    try:
        response = requests.get(f"{BACKEND_URL}/metrics", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"HTTP {response.status_code}"}
    except requests.exceptions.ConnectionError:
        return {"error": "connection_failed"}
    except requests.exceptions.Timeout:
        return {"error": "timeout"}
    except Exception as e:
        return {"error": str(e)}

def fetch_datasets(tier=None):
    """Fetch datasets from backend with graceful error handling"""
    try:
        url = f"{BACKEND_URL}/datasets"
        if tier:
            url += f"?tier={tier}"
        
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return response.json()
        return []
    except Exception:
        return []

def fetch_movements(limit=50):
    """Fetch recent movements from backend with graceful error handling"""
    try:
        response = requests.get(f"{BACKEND_URL}/movements?limit={limit}", timeout=5)
        if response.status_code == 200:
            return response.json()
        return []
    except Exception:
        return []

def format_bytes(bytes_value):
    """Format bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"

def trigger_decision(dataset_id, auto_execute=True):
    """Trigger ML decision for a dataset"""
    try:
        response = requests.post(
            f"{BACKEND_URL}/decide",
            json={"dataset_id": dataset_id, "auto_execute": auto_execute},
            timeout=10
        )
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Failed to trigger decision: {e}")
    return None

def tier_style(value: str) -> str:
    """Return CSS styles for tier badges inside dataframes"""
    palette = {
        "hot": {"bg": "#fdecea", "fg": "#c53030"},
        "warm": {"bg": "#fff7e6", "fg": "#975a16"},
        "cold": {"bg": "#e0f2f1", "fg": "#0f766e"}
    }
    if not value:
        return ""
    config = palette.get(value.lower(), {"bg": "#edf2f7", "fg": "#1a202c"})
    return f"background-color:{config['bg']}; color:{config['fg']}; font-weight:600;"

def status_style(value: str) -> str:
    """Return CSS styles for movement status badges"""
    palette = {
        "completed": {"bg": "#dcfce7", "fg": "#15803d"},
        "pending": {"bg": "#fef9c3", "fg": "#854d0e"},
        "in_progress": {"bg": "#fef9c3", "fg": "#854d0e"},
        "failed": {"bg": "#fee2e2", "fg": "#b91c1c"}
    }
    if not value:
        return ""
    normalized = value.lower().replace(" ", "_")
    config = palette.get(normalized, {"bg": "#edf2f7", "fg": "#1a202c"})
    return f"background-color:{config['bg']}; color:{config['fg']}; font-weight:600;"

# Retrieve model metadata once for sidebar display
model_info = None
try:
    model_response = requests.get(f"{BACKEND_URL}/ml/model-info", timeout=5)
    if model_response.status_code == 200:
        model_info = model_response.json()
except Exception:
    model_info = None

# Header
st.markdown('<h1 class="main-header">Intelligent Cloud Storage Dashboard</h1>', unsafe_allow_html=True)
st.markdown(
    f"""
    <div style="text-align:center;">
        <div class="team-badge">
            <span>Team</span> {TEAM_NAME}
        </div>
        <p style="margin:0; color:#5f6b7c; font-size:1rem;">
            Built with grit by <strong>{BUILDER_NAME}</strong> - {BUILDER_ID}
        </p>
    </div>
    """,
    unsafe_allow_html=True
)
st.markdown("---")

# Sidebar
with st.sidebar:
    st.markdown("### Operations Console", unsafe_allow_html=True)
    auto_refresh = st.checkbox("Enable auto-refresh", value=True)
    refresh_interval = st.slider("Refresh cadence (seconds)", 5, 60, 10)
    st.caption("Refresh interval applies when auto-refresh is enabled.")
    
    st.divider()
    st.markdown("### Dataset Filters")
    tier_options = ["All", "hot", "warm", "cold"]
    selected_tier = st.selectbox(
        "Storage tier",
        tier_options,
        format_func=lambda x: "All tiers" if x == "All" else x.capitalize()
    )
    st.caption("Dial in what matters for watched_underdog's live operations.")
    
    st.divider()
    st.markdown("### ML Automation")
    if st.button("Retrain ML model"):
        try:
            response = requests.post(f"{BACKEND_URL}/ml/retrain", timeout=30)
            if response.status_code == 200:
                st.success("Model retrained successfully.")
            else:
                st.error("Failed to retrain model.")
        except Exception as e:
            st.error(f"Error: {e}")
    
    if model_info:
        status_text = "Available" if model_info.get("available") else "Unavailable"
        st.metric("Model status", status_text)
        st.markdown(f"**Type:** {model_info.get('model_type', '-')}")
        features = model_info.get("features") or []
        tiers = model_info.get("tiers") or []
        if features:
            st.caption("Features: " + " - ".join(features))
        if tiers:
            st.caption("Tiers: " + ", ".join(tiers))
        st.caption(f"Path: {model_info.get('model_path', '-')}")
    else:
        st.warning("Model metadata unavailable.")
    
    st.divider()
    st.markdown("### Credits")
    st.markdown(
        f"Crafted solely by **{BUILDER_NAME} ({BUILDER_ID})** "
        f"for Team **{TEAM_NAME}**."
    )

# Fetch data
metrics = fetch_metrics()
datasets = fetch_datasets(None if selected_tier == "All" else selected_tier)
movements = fetch_movements(limit=50)

# Show connection status
if isinstance(metrics, dict) and "error" in metrics:
    st.error(f"Cannot connect to backend at {BACKEND_URL}")
    st.info("Please ensure the backend service is running. Try refreshing the page.")
    if st.button("Retry Connection"):
        st.rerun()
    st.stop()

if metrics:
    # Metrics Overview
    st.header("System Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Datasets",
            value=metrics['total_datasets']
        )
    
    with col2:
        st.metric(
            label="Total Storage",
            value=format_bytes(metrics['total_size_bytes'])
        )
    
    with col3:
        st.metric(
            label="Estimated Cost",
            value=f"${metrics['estimated_cost_usd']:.2f}/month"
        )
    
    with col4:
        movement_success_rate = (
            metrics['successful_movements'] / metrics['total_movements'] * 100
            if metrics['total_movements'] > 0 else 0
        )
        st.metric(
            label="Movement Success Rate",
            value=f"{movement_success_rate:.1f}%"
        )
    
    st.markdown("---")
    
    # Tier Distribution
    st.header("Tier Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Pie chart by count
        tier_data = pd.DataFrame(metrics['tier_distribution'])
        
        fig_count = px.pie(
            tier_data,
            values='count',
            names='tier',
            title='Datasets by Tier (Count)',
            color='tier',
            color_discrete_map={'hot': '#e53e3e', 'warm': '#dd6b20', 'cold': '#0d9488'}
        )
        fig_count.update_traces(textposition='inside', textinfo='percent+label+value')
        st.plotly_chart(fig_count, use_container_width=True)
    
    with col2:
        # Pie chart by size
        fig_size = px.pie(
            tier_data,
            values='size_bytes',
            names='tier',
            title='Storage by Tier (Size)',
            color='tier',
            color_discrete_map={'hot': '#e53e3e', 'warm': '#dd6b20', 'cold': '#0d9488'}
        )
        fig_size.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_size, use_container_width=True)
    
    # Bar chart for tier comparison
    tier_data['size_gb'] = tier_data['size_bytes'] / (1024**3)
    
    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(
        x=tier_data['tier'],
        y=tier_data['count'],
        name='Count',
        marker_color=['#e53e3e', '#dd6b20', '#0d9488'],
        text=tier_data['count'],
        textposition='auto'
    ))
    
    fig_bar.update_layout(
        title='Dataset Count by Tier',
        xaxis_title='Tier',
        yaxis_title='Number of Datasets',
        showlegend=False
    )
    
    st.plotly_chart(fig_bar, use_container_width=True)
    
    summary_parts = [
        f"{row['tier'].title()}: {row['count']} datasets / {format_bytes(row['size_bytes'])}"
        for _, row in tier_data.iterrows()
    ]
    st.caption("Composition snapshot - " + " | ".join(summary_parts))
    
    st.markdown("---")

# Datasets Table
st.header("Datasets")

# Add search and filters
col1, col2 = st.columns([3, 1])
with col1:
    search_term = st.text_input("Search datasets", placeholder="Enter dataset name...")
with col2:
    sort_by = st.selectbox("Sort by", ["name", "size_bytes", "access_freq_7d", "avg_latency_ms"])

if datasets:
    df_datasets = pd.DataFrame(datasets)
    
    # Apply search filter
    if search_term:
        df_datasets = df_datasets[df_datasets['name'].str.contains(search_term, case=False, na=False)]
    
    # Sort datasets
    df_datasets = df_datasets.sort_values(by=sort_by, ascending=False)
    
    # Format columns
    df_datasets['size_formatted'] = df_datasets['size_bytes'].apply(format_bytes)
    df_datasets['last_accessed'] = pd.to_datetime(df_datasets['last_accessed']).dt.strftime('%Y-%m-%d %H:%M')
    df_datasets['latency_formatted'] = df_datasets['avg_latency_ms'].apply(lambda x: f"{x:.1f} ms")
    df_datasets['tier_label'] = df_datasets['current_tier'].str.upper()
    
    display_df = df_datasets[['name', 'tier_label', 'size_formatted', 'access_freq_7d', 'latency_formatted', 'last_accessed']].rename(columns={
        'name': 'Dataset Name',
        'tier_label': 'Tier',
        'size_formatted': 'Size',
        'access_freq_7d': 'Access Count (7d)',
        'latency_formatted': 'Avg Latency',
        'last_accessed': 'Last Accessed'
    })
    
    styled_df = display_df.style.applymap(tier_style, subset=['Tier'])
    st.dataframe(
        styled_df,
        use_container_width=True,
        height=400
    )
    
    st.caption(f"Showing {len(df_datasets)} of {len(datasets)} datasets")
    
    # Dataset actions
    st.subheader("Trigger ML Decision")
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        selected_dataset = st.selectbox(
            "Select dataset",
            options=df_datasets['id'].tolist(),
            format_func=lambda x: df_datasets[df_datasets['id'] == x]['name'].iloc[0]
        )
    
    with col2:
        auto_execute = st.checkbox("Auto-execute", value=True)
    
    with col3:
        if st.button("Decide Tier"):
            result = trigger_decision(selected_dataset, auto_execute)
            if result:
                st.success(f"Decision: {result['recommended_tier']} (confidence: {result['confidence']:.2%})")
                if result['executed']:
                    st.info(f"Movement initiated: {result['current_tier']} -> {result['recommended_tier']}")
                
                # Clear cache to refresh data
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
else:
    st.info("No datasets found")

st.markdown("---")

# Movements History
st.header("Recent Movements")

if movements:
    df_movements = pd.DataFrame(movements)
    
    # Format columns
    df_movements['size_formatted'] = df_movements['size_bytes'].apply(format_bytes)
    df_movements['initiated_at'] = pd.to_datetime(df_movements['initiated_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df_movements['migration'] = df_movements['from_tier'] + ' -> ' + df_movements['to_tier']
    
    df_movements['status_label'] = df_movements['status'].apply(lambda x: x.replace('_', ' ').title())
    df_movements['error_indicator'] = df_movements['error_message'].apply(
        lambda x: "Error" if x and len(str(x)) > 0 else ""
    )
    
    # Display table
    display_movements = df_movements[['dataset_name', 'migration', 'size_formatted', 'reason', 'status_label', 'error_indicator', 'initiated_at']].rename(columns={
        'dataset_name': 'Dataset',
        'migration': 'Migration',
        'size_formatted': 'Size',
        'reason': 'Reason',
        'status_label': 'Status',
        'error_indicator': 'Errors',
        'initiated_at': 'Initiated At'
    })
    
    styled_movements = display_movements.style.applymap(status_style, subset=['Status'])
    st.dataframe(
        styled_movements,
        use_container_width=True,
        height=400
    )
    
    # Show error details for failed movements
    failed_movements = df_movements[df_movements['status'] == 'failed']
    if not failed_movements.empty:
        with st.expander("View Error Details"):
            for _, mov in failed_movements.iterrows():
                st.error(f"**{mov['dataset_name']}**: {mov.get('error_message', 'Unknown error')}")
    
    # Movement statistics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        completed = len(df_movements[df_movements['status'] == 'completed'])
        st.metric("Completed", completed)
    
    with col2:
        pending = len(df_movements[df_movements['status'].isin(['pending', 'in_progress'])])
        st.metric("Pending/In Progress", pending)
    
    with col3:
        failed = len(df_movements[df_movements['status'] == 'failed'])
        st.metric("Failed", failed)
    
    # Movement flow visualization
    if not df_movements.empty:
        movement_counts = df_movements.groupby('migration').size().reset_index(name='count')
        
        fig_movements = px.bar(
            movement_counts,
            x='migration',
            y='count',
            title='Movement Patterns',
            color='count',
            color_continuous_scale='Viridis',
            text='count'
        )
        fig_movements.update_traces(textposition='auto')
        fig_movements.update_layout(showlegend=False)
        
        st.plotly_chart(fig_movements, use_container_width=True)

else:
    st.info("No movements recorded yet")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray;'>
        <p>Intelligent Cloud Storage System | Team watched_underdog</p>
        <p>Built end-to-end by Adhik Agarwal - 2022ABPS1364P</p>
        <p>Last updated: {}</p>
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    unsafe_allow_html=True
)

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
