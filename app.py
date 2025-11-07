# app.py
import streamlit as st
import asyncio
import pandas as pd
from agent import run_agent
from dashboard_agent import run_agent as run_dashboard_agent
from data_manager import data_manager

# Helper function to get config values
def get_config(key: str, default: str = "") -> str:
    """Get config from environment or Streamlit secrets"""
    import os
    # First try environment variables
    value = os.getenv(key)
    if value:
        return value
    
    # Then try Streamlit secrets
    try:
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except:
        pass
    
    return default

# Check if API key is configured
api_key = get_config("MODEL_API_KEY")
if not api_key:
    st.error("⚠️ MODEL_API_KEY not configured. Please add it to Streamlit secrets.")
    st.info("""
    To add secrets:
    1. Go to your app settings on Streamlit Cloud
    2. Click on "Secrets"
    3. Add: MODEL_API_KEY = "your_groq_api_key"
    """)
    st.stop()

# Create an event loop for async operations
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

st.set_page_config(
    page_title="Advanced Data Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Title and Header
st.title("📊 Advanced Data Analytics Dashboard")
st.write("Upload your datasets, analyze with AI, and create interactive dashboards.")

# Initialize data manager
data_manager.connect()

# Sidebar with dataset management
with st.sidebar:
    st.header("📂 Dataset Management")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload Dataset",
        type=['csv', 'xlsx', 'xls', 'json'],
        help="Upload CSV, Excel, or JSON files"
    )
    
    dataset_name = st.text_input("Dataset Name", placeholder="Enter a name for this dataset")
    
    if uploaded_file and dataset_name:
        if st.button("Upload Dataset"):
            with st.spinner("Processing dataset..."):
                result = data_manager.upload_dataset(uploaded_file, dataset_name)
                if "error" in result:
                    st.error(f"Upload failed: {result['error']}")
                else:
                    st.success(f"✅ Dataset '{dataset_name}' uploaded successfully!")
                    st.json({
                        "Rows": result['row_count'],
                        "Columns": result['column_count'],
                        "Columns": result['columns']
                    })
    
    # Dataset selection
    available_datasets = data_manager.get_dataset_names()
    if available_datasets:
        st.subheader("Select Dataset")
        selected_dataset = st.selectbox(
            "Choose dataset for analysis:",
            available_datasets,
            index=0
        )
        
        if selected_dataset:
            data_manager.set_current_dataset(selected_dataset)
            dataset_info = data_manager.get_dataset_info(selected_dataset)
            
            st.info(f"""
            **Current Dataset:** {selected_dataset}
            - 📈 {dataset_info['row_count']} rows
            - 🗂️ {dataset_info['column_count']} columns
            - 📝 {len([col for col in dataset_info['columns']])} features
            """)
    
    st.markdown("---")
    st.header("ℹ️ How to Use")
    st.markdown("""
    1. **Upload** your dataset (CSV, Excel, JSON)
    2. **Select** the dataset for analysis
    3. **Chat** with AI about your data
    4. **Generate** interactive dashboards
    5. **Download** insights and visualizations
    """)

def display_html_dashboard(html_content: str):
    """Displays the HTML dashboard in Streamlit."""
    st.components.v1.html(html_content, height=1200, scrolling=True)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "dashboard_html" not in st.session_state:
    st.session_state.dashboard_html = ""
if "current_dataset" not in st.session_state:
    st.session_state.current_dataset = None

# Update current dataset from sidebar
if available_datasets:
    st.session_state.current_dataset = selected_dataset

# Tabs
tab1, tab2, tab3 = st.tabs(["💬 AI Chat", "📈 Dashboard", "🔍 Data Explorer"])

# Chatbot Tab
with tab1:
    st.header("💬 AI Data Analyst")
    
    if not available_datasets:
        st.warning("⚠️ Please upload a dataset first using the sidebar.")
    else:
        chat_container = st.container()

        # Display chat history
        with chat_container:
            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])
                    
                    # Show metadata if available
                    if "metadata" in message:
                        with st.expander("Query Details"):
                            st.json(message["metadata"])

        # User input
        user_query = st.chat_input(f"Ask about your {selected_dataset} data...")

        if user_query:
            st.session_state.messages.append({"role": "user", "content": user_query})

            with chat_container:
                with st.chat_message("user"):
                    st.markdown(user_query)

            with st.spinner("🤔 Analyzing your data..."):
                try:
                    resp = loop.run_until_complete(run_agent(user_query, dataset_name=selected_dataset))
                except Exception as e:
                    st.error(f"An error occurred: {e}")
                    resp = {"content": f"Error: {str(e)}"}

            if resp:
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": resp["content"],
                    "metadata": resp.get("metadata", {})
                })
                with chat_container:
                    with st.chat_message("assistant"):
                        st.markdown(resp["content"])
                        
                        # Show query details
                        if "metadata" in resp and "query_used" in resp["metadata"]:
                            with st.expander("🔍 See analysis details"):
                                st.write("**SQL Query Used:**")
                                st.code(resp["metadata"]["query_used"], language="sql")
                                st.write("**Dataset:**", resp["metadata"].get("dataset", "N/A"))
                                st.write("**Results Count:**", resp["metadata"].get("results_count", "N/A"))

        # Clear chat button
        if st.button("Clear Conversation"):
            st.session_state.messages = []
            st.rerun()

# Dashboard Tab
with tab2:
    st.header("📈 Interactive Dashboard")
    
    if not available_datasets:
        st.warning("⚠️ Please upload a dataset first using the sidebar.")
    else:
        # Manual selection interface
        st.subheader("🎯 Custom Chart Selection")
        
        df = data_manager.datasets[selected_dataset]['dataframe']
        all_columns = df.columns.tolist()
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Select Columns for Charts:**")
            selected_columns = st.multiselect(
                "Choose columns to visualize:",
                all_columns,
                help="Select up to 6 columns for visualization",
                key="column_selector"
            )
        
        with col2:
            st.write("**Chart Types:**")
            chart_types = []
            
            # Create unique keys for each selectbox
            for i, col in enumerate(selected_columns):
                # Create a unique key based on column name and index
                unique_key = f"chart_type_{selected_dataset}_{col}_{i}"
                
                # Determine available chart types based on data type
                if col in numeric_cols:
                    available_types = ["histogram", "box", "scatter", "line", "violin"]
                    default_idx = 0
                else:
                    available_types = ["bar", "pie"]
                    default_idx = 0
                    
                chart_type = st.selectbox(
                    f"Chart type for {col}:",
                    available_types,
                    index=default_idx,
                    key=unique_key
                )
                
                chart_types.append(chart_type)
            
            # Show scatter plot help
            if "scatter" in chart_types:
                scatter_indices = [i for i, ct in enumerate(chart_types) if ct == "scatter"]
                for idx in scatter_indices:
                    if idx + 1 >= len(selected_columns):
                        st.warning(
                            f"⚠️ Scatter plot for '{selected_columns[idx]}' needs another numeric column. Please select one more numeric column."
                        )
                    elif selected_columns[idx + 1] not in numeric_cols:
                        st.warning(
                            f"⚠️ Scatter plot for '{selected_columns[idx]}' needs a numeric column, but '{selected_columns[idx + 1]}' is not numeric."
                        )
        
        # Show column information
        with st.expander("📊 Column Information"):
            st.write(f"**Numeric Columns ({len(numeric_cols)}):** {', '.join(numeric_cols)}")
            st.write(f"**Categorical Columns ({len(categorical_cols)}):** {', '.join(categorical_cols)}")
            st.info("""
            **Chart Types Explained:**
            - **Histogram**: Shows distribution of numeric data
            - **Box**: Shows quartiles and outliers
            - **Scatter**: Shows relationship between two numeric columns
            - **Line**: Shows trends over time or sequence
            - **Violin**: Shows distribution shape with more detail
            - **Bar**: Shows counts of categorical values
            - **Pie**: Shows proportions of categories
            """)
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            if st.button("🚀 Generate Advanced Dashboard", type="primary", use_container_width=True, key="generate_dashboard_btn"):
                with st.spinner("Creating interactive dashboard..."):
                    try:
                        # Use manual selection if provided, otherwise auto-generate
                        if selected_columns:
                            dashboard_response = loop.run_until_complete(
                                run_dashboard_agent(selected_dataset, selected_columns, chart_types)
                            )
                        else:
                            dashboard_response = loop.run_until_complete(run_dashboard_agent(selected_dataset))
                            
                        html_content = dashboard_response.get("content", "")
                        
                        # Clean HTML content
                        if html_content.startswith("```html"):
                            html_content = html_content.replace("```html", "")
                        if html_content.endswith("```"):
                            html_content = html_content.replace("```", "")
                        
                        # Store in session state
                        st.session_state.dashboard_html = html_content
                        st.session_state.dashboard_metadata = dashboard_response.get("metadata", {})
                        st.session_state.dashboard_generated = True
                        
                        st.success("✅ Dashboard generated successfully!")
                        
                    except Exception as e:
                        st.error(f"An error occurred: {e}")
        
        with col2:
            if st.session_state.get('dashboard_html'):
                st.download_button(
                    label="📥 Download Dashboard",
                    data=st.session_state.dashboard_html,
                    file_name=f"{selected_dataset}_dashboard.html",
                    mime="text/html",
                    use_container_width=True,
                    key="download_dashboard_btn"
                )
        
        with col3:
            if st.session_state.get('dashboard_html'):
                if st.button("🔄 Refresh Dashboard", use_container_width=True, key="refresh_dashboard_btn"):
                    # Clear dashboard to allow regeneration
                    st.session_state.dashboard_html = ""
                    st.session_state.dashboard_metadata = {}
                    st.rerun()
        
        # Display dashboard metadata if available
        if st.session_state.get('dashboard_metadata'):
            with st.expander("📊 Dashboard Information"):
                st.write("**Dataset:**", st.session_state.dashboard_metadata.get('dataset', 'N/A'))
                st.write("**Charts Generated:**", st.session_state.dashboard_metadata.get('charts_generated', 0))
                if st.session_state.dashboard_metadata.get('selected_columns'):
                    st.write("**Selected Columns:**", ", ".join(st.session_state.dashboard_metadata['selected_columns']))
                    if st.session_state.dashboard_metadata.get('chart_types'):
                        st.write("**Chart Types:**", ", ".join(st.session_state.dashboard_metadata['chart_types']))
        
        # Display the dashboard
        if st.session_state.get('dashboard_html'):
            display_html_dashboard(st.session_state.dashboard_html)
        else:
            st.info("👆 Click 'Generate Advanced Dashboard' to create an interactive visualization dashboard. You can optionally select specific columns and chart types above.")

with tab3:
    st.header("🔍 Data Explorer")
    
    if not available_datasets:
        st.warning("⚠️ Please upload a dataset first using the sidebar.")
    else:
        dataset_info = data_manager.get_dataset_info(selected_dataset)
        analysis = data_manager.get_analysis(selected_dataset)
        
        # Basic info
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Rows", dataset_info['row_count'])
        with col2:
            st.metric("Total Columns", dataset_info['column_count'])
        with col3:
            st.metric("Missing Values", analysis['basic_stats']['missing_values'])
        with col4:
            st.metric("Duplicate Rows", analysis['basic_stats']['duplicate_rows'])
        
        # Data preview
        st.subheader("Data Preview")
        df = data_manager.datasets[selected_dataset]['dataframe']
        st.dataframe(df.head(20), use_container_width=True)
        
        # Column analysis
        st.subheader("Column Analysis")
        for col, info in analysis['column_analysis'].items():
            with st.expander(f"📊 {col} ({info['dtype']})"):
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Basic Stats:**")
                    st.json({
                        "Unique Values": info['unique_values'],
                        "Missing Values": info['missing_values'],
                        "Sample Values": info['sample_values'][:5] if info['sample_values'] else []
                    })
                with col2:
                    if 'mean' in info:
                        st.write("**Numeric Statistics:**")
                        st.json({
                            "Mean": round(info['mean'], 2),
                            "Median": round(info['median'], 2),
                            "Standard Deviation": round(info['std'], 2),
                            "Min": info['min'],
                            "Max": info['max']
                        })
        
        # Quick analysis
        st.subheader("Quick Insights")
        if 'correlations' in analysis and analysis['correlations']:
            st.write("**Top Correlations:**")
            # Display correlation matrix
            numeric_df = df.select_dtypes(include=['number'])
            if len(numeric_df.columns) > 1:
                st.dataframe(numeric_df.corr(), use_container_width=True)

# Footer
st.markdown("---")
st.markdown("👨‍💻 *Advanced Data Analytics Dashboard*")