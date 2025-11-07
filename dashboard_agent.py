# dashboard_agent.py
import asyncio
import os
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
import numpy as np
import base64
from io import BytesIO
from typing import Dict, Any, List
from data_manager import data_manager

# Configure Plotly for better compatibility
import plotly.io as pio
pio.templates.default = "plotly_white"

async def run_agent(dataset_name: str = None, selected_columns: List[str] = None, chart_types: List[str] = None) -> Dict[str, Any]:
    """Generate advanced dashboard with Plotly visualizations"""
    try:
        dataset_info = data_manager.get_dataset_info(dataset_name)
        
        if "error" in dataset_info:
            return {"content": "<html><body><h1>Please upload a dataset first</h1></body></html>"}
        
        # Get dataset analysis
        analysis = data_manager.get_analysis(dataset_name)
        df = data_manager.datasets[dataset_name]['dataframe']
        
        print(f"DEBUG: Dataset shape: {df.shape}")
        print(f"DEBUG: Dataset info keys: {dataset_info.keys()}")
        print(f"DEBUG: Analysis keys: {analysis.keys() if analysis else 'No analysis'}")
        print(f"DEBUG: Selected columns: {selected_columns}")
        print(f"DEBUG: Chart types: {chart_types}")
        
        # Generate visualizations
        html_content = generate_advanced_dashboard(df, dataset_info, analysis, selected_columns, chart_types)
        
        return {
            "content": html_content,
            "metadata": {
                "dataset": dataset_info.get('name', 'Unknown'),
                "charts_generated": count_charts_in_html(html_content),
                "columns": dataset_info.get('columns', []),
                "row_count": dataset_info.get('row_count', 0),
                "column_count": dataset_info.get('column_count', 0),
                "selected_columns": selected_columns,
                "chart_types": chart_types,
                "analysis_complete": True
            }
        }
        
    except Exception as e:
        print(f"DEBUG: Error in run_agent: {e}")
        import traceback
        traceback.print_exc()
        return {"content": f"<html><body><h1>Error</h1><p>{str(e)}</p><pre>{traceback.format_exc()}</pre></body></html>"}

def plotly_to_image(fig):
    """Convert Plotly figure to base64 image with better error handling"""
    try:
        # Convert to image bytes with higher quality
        img_bytes = fig.to_image(format="png", width=1000, height=500, scale=2)
        # Encode to base64
        img_base64 = base64.b64encode(img_bytes).decode('utf-8')
        return f"data:image/png;base64,{img_base64}"
    except Exception as e:
        print(f"DEBUG: Error in plotly_to_image: {e}")
        return None
def create_empty_chart(message: str, chart_id: str, title: str = "Chart Error") -> str:
    """Create a placeholder for charts that failed to generate"""
    return f"""
    <div class="chart-container">
        <h3 class="text-xl font-bold text-gray-800 mb-4">{title}</h3>
        <div class="flex items-center justify-center h-96 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">
            <div class="text-center">
                <div class="text-5xl mb-4">📊</div>
                <p class="text-gray-600 text-lg mb-2">Chart Generation Failed</p>
                <p class="text-gray-500 text-sm">{message}</p>
            </div>
        </div>
        <div class="mt-4 text-sm text-gray-600">
            <p><strong>Chart ID:</strong> {chart_id}</p>
        </div>
    </div>
    """
def create_beautiful_chart(chart_type: str, df: pd.DataFrame, x_col: str, y_col: str = None, z_col: str = None, title: str = ""):
    """Create beautiful charts with consistent styling"""
    try:
        # Common layout settings
        common_layout = dict(
            plot_bgcolor='white',
            paper_bgcolor='white',
            height=500,
            margin=dict(l=60, r=40, t=80, b=60),
            font=dict(size=12),
            title=dict(
                text=title,
                x=0.5,
                xanchor='center',
                font=dict(size=18, color='#1f2937')
            )
        )
        
        if chart_type == 'histogram' and x_col in df.columns:
            # Clean data for histogram
            clean_data = df[x_col].dropna()
            if len(clean_data) == 0:
                return None
                
            fig = px.histogram(
                x=clean_data,
                title=title,
                color_discrete_sequence=['#3B82F6'],
                nbins=30,
                opacity=0.85
            )
            
            # Enhanced styling for histogram
            fig.update_traces(
                marker=dict(
                    line=dict(width=1.5, color='white'),
                    opacity=0.85
                ),
                hovertemplate=f'<b>{x_col}</b>: %{{x}}<br>Count: %{{y}}<extra></extra>'
            )
            
            fig.update_layout(
                showlegend=False,
                **common_layout,
                xaxis=dict(
                    title=dict(text=x_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                ),
                yaxis=dict(
                    title=dict(text="Count", font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                )
            )
            return fig
            
        elif chart_type == 'bar' and x_col in df.columns:
            # For categorical data, show value counts
            value_counts = df[x_col].value_counts().head(12)
            if len(value_counts) == 0:
                return None
                
            fig = px.bar(
                x=value_counts.index.astype(str),
                y=value_counts.values,
                title=title,
                color=value_counts.values,
                color_continuous_scale='viridis'
            )
            
            # Enhanced styling for bar chart
            fig.update_traces(
                marker=dict(
                    line=dict(width=1.5, color='white'),
                    opacity=0.9
                ),
                hovertemplate='<b>%{x}</b><br>Count: %{y}<extra></extra>'
            )
            
            fig.update_layout(
                showlegend=False,
                **common_layout,
                xaxis_title=x_col,
                yaxis_title="Count",
                xaxis=dict(
                    title=dict(text=x_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    tickangle=45
                ),
                yaxis=dict(
                    title=dict(text="Count", font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                )
            )
            return fig
            
        elif chart_type == 'scatter' and x_col in df.columns and y_col in df.columns:
            # Clean data for scatter plot
            clean_df = df[[x_col, y_col]].dropna()
            if len(clean_df) == 0:
                return None
                
            fig = px.scatter(
                clean_df,
                x=x_col,
                y=y_col,
                title=title,
                color_discrete_sequence=['#10B981'],
                opacity=0.7
            )
            
            # Enhanced styling for scatter plot
            fig.update_traces(
                marker=dict(
                    size=8,
                    line=dict(width=1, color='white'),
                    opacity=0.7
                ),
                hovertemplate=f'<b>{x_col}</b>: %{{x}}<br><b>{y_col}</b>: %{{y}}<extra></extra>'
            )
            
            fig.update_layout(
                **common_layout,
                xaxis=dict(
                    title=dict(text=x_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                ),
                yaxis=dict(
                    title=dict(text=y_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                )
            )
            return fig
            
        elif chart_type == 'box' and x_col in df.columns:
            # Clean data for box plot
            clean_data = df[x_col].dropna()
            if len(clean_data) == 0:
                return None
                
            fig = px.box(
                y=clean_data,
                title=title,
                color_discrete_sequence=['#8B5CF6']
            )
            
            # Enhanced styling for box plot
            fig.update_traces(
                marker=dict(
                    size=4,
                    opacity=0.7
                ),
                line=dict(width=2),
                hovertemplate=f'<b>{x_col}</b>: %{{y}}<extra></extra>'
            )
            
            fig.update_layout(
                showlegend=False,
                **common_layout,
                yaxis=dict(
                    title=dict(text=x_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                )
            )
            return fig
            
        elif chart_type == 'line' and x_col in df.columns and y_col in df.columns:
            # For line chart, we need sorted data
            clean_df = df[[x_col, y_col]].dropna().sort_values(by=x_col)
            if len(clean_df) == 0:
                return None
                
            fig = px.line(
                clean_df,
                x=x_col,
                y=y_col,
                title=title,
                color_discrete_sequence=['#EF4444']
            )
            
            # Enhanced styling for line chart
            fig.update_traces(
                line=dict(width=3),
                marker=dict(size=6),
                hovertemplate=f'<b>{x_col}</b>: %{{x}}<br><b>{y_col}</b>: %{{y}}<extra></extra>'
            )
            
            fig.update_layout(
                **common_layout,
                xaxis=dict(
                    title=dict(text=x_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                ),
                yaxis=dict(
                    title=dict(text=y_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                )
            )
            return fig
            
        elif chart_type == 'area' and x_col in df.columns and y_col in df.columns:
            # For area chart
            clean_df = df[[x_col, y_col]].dropna().sort_values(by=x_col)
            if len(clean_df) == 0:
                return None
                
            fig = px.area(
                clean_df,
                x=x_col,
                y=y_col,
                title=title,
                color_discrete_sequence=['#F59E0B']
            )
            
            fig.update_traces(
                hovertemplate=f'<b>{x_col}</b>: %{{x}}<br><b>{y_col}</b>: %{{y}}<extra></extra>'
            )
            
            fig.update_layout(
                **common_layout,
                xaxis=dict(
                    title=dict(text=x_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                ),
                yaxis=dict(
                    title=dict(text=y_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                )
            )
            return fig
            
        elif chart_type == 'pie' and x_col in df.columns:
            # For pie chart - show value counts
            value_counts = df[x_col].value_counts().head(8)
            if len(value_counts) == 0:
                return None
                
            fig = px.pie(
                values=value_counts.values,
                names=value_counts.index.astype(str),
                title=title,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
            )
            
            fig.update_layout(
                **common_layout,
                showlegend=False
            )
            return fig
            
        elif chart_type == 'heatmap' and len(df.select_dtypes(include=['number']).columns) >= 2:
            # Correlation heatmap for numeric columns
            numeric_df = df.select_dtypes(include=['number'])
            if len(numeric_df.columns) < 2:
                return None
                
            corr_matrix = numeric_df.corr()
            
            fig = px.imshow(
                corr_matrix,
                title=title,
                color_continuous_scale='RdBu_r',
                aspect='auto'
            )
            
            fig.update_layout(
                **common_layout,
                xaxis_title="Columns",
                yaxis_title="Columns"
            )
            return fig
            
        elif chart_type == 'violin' and x_col in df.columns:
            # Violin plot for distribution
            clean_data = df[x_col].dropna()
            if len(clean_data) == 0:
                return None
                
            fig = px.violin(
                y=clean_data,
                title=title,
                color_discrete_sequence=['#EC4899']
            )
            
            fig.update_traces(
                hovertemplate=f'<b>{x_col}</b>: %{{y}}<extra></extra>'
            )
            
            fig.update_layout(
                showlegend=False,
                **common_layout,
                yaxis=dict(
                    title=dict(text=x_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                )
            )
            return fig
            
        elif chart_type == 'density_contour' and x_col in df.columns and y_col in df.columns:
            # Density contour plot
            clean_df = df[[x_col, y_col]].dropna()
            if len(clean_df) == 0:
                return None
                
            fig = px.density_contour(
                clean_df,
                x=x_col,
                y=y_col,
                title=title,
                color_continuous_scale='Viridis'
            )
            
            fig.update_traces(
                contours_coloring="fill",
                contours_showlabels=True
            )
            
            fig.update_layout(
                **common_layout,
                xaxis=dict(
                    title=dict(text=x_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                ),
                yaxis=dict(
                    title=dict(text=y_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                )
            )
            return fig
            
        elif chart_type == 'bubble' and x_col in df.columns and y_col in df.columns and z_col in df.columns:
            # Bubble chart with three dimensions
            clean_df = df[[x_col, y_col, z_col]].dropna()
            if len(clean_df) == 0:
                return None
                
            fig = px.scatter(
                clean_df,
                x=x_col,
                y=y_col,
                size=z_col,
                title=title,
                color=z_col,
                color_continuous_scale='Plasma'
            )
            
            fig.update_traces(
                marker=dict(
                    sizemode='diameter',
                    sizeref=2.*clean_df[z_col].max()/(40.**2),
                    line=dict(width=1, color='white')
                ),
                hovertemplate=f'<b>{x_col}</b>: %{{x}}<br><b>{y_col}</b>: %{{y}}<br><b>{z_col}</b>: %{{marker.size}}<extra></extra>'
            )
            
            fig.update_layout(
                **common_layout,
                xaxis=dict(
                    title=dict(text=x_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                ),
                yaxis=dict(
                    title=dict(text=y_col, font=dict(size=14)),
                    gridcolor='#f3f4f6',
                    gridwidth=1
                )
            )
            return fig
            
        elif chart_type == 'treemap' and x_col in df.columns and y_col in df.columns:
            # Treemap for hierarchical data
            clean_df = df[[x_col, y_col]].dropna()
            if len(clean_df) == 0:
                return None
                
            # Aggregate data for treemap
            aggregated = clean_df.groupby(x_col)[y_col].sum().reset_index()
            
            fig = px.treemap(
                aggregated,
                path=[x_col],
                values=y_col,
                title=title,
                color=y_col,
                color_continuous_scale='Blues'
            )
            
            fig.update_layout(
                **common_layout
            )
            return fig
            
    except Exception as e:
        print(f"DEBUG: Error in create_beautiful_chart ({chart_type}): {e}")
        return None
    
    return None

def generate_advanced_dashboard(df: pd.DataFrame, dataset_info: dict, analysis: dict, selected_columns: List[str] = None, chart_types: List[str] = None) -> str:
    """Generate a comprehensive dashboard with vertical layout"""
    
    # Create visualizations in vertical layout
    charts_html = create_vertical_visualizations(df, analysis, selected_columns, chart_types)
    
    # Dataset overview
    overview_html = f"""
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <div class="bg-blue-50 border border-blue-200 rounded-lg p-4 text-center stat-card">
            <div class="text-2xl font-bold text-blue-600">{dataset_info['row_count']}</div>
            <div class="text-sm text-blue-800">Total Rows</div>
        </div>
        <div class="bg-green-50 border border-green-200 rounded-lg p-4 text-center stat-card">
            <div class="text-2xl font-bold text-green-600">{dataset_info['column_count']}</div>
            <div class="text-sm text-green-800">Total Columns</div>
        </div>
        <div class="bg-purple-50 border border-purple-200 rounded-lg p-4 text-center stat-card">
            <div class="text-2xl font-bold text-purple-600">{analysis['basic_stats']['missing_values']}</div>
            <div class="text-sm text-purple-800">Missing Values</div>
        </div>
        <div class="bg-orange-50 border border-orange-200 rounded-lg p-4 text-center stat-card">
            <div class="text-2xl font-bold text-orange-600">{analysis['basic_stats']['duplicate_rows']}</div>
            <div class="text-sm text-orange-800">Duplicate Rows</div>
        </div>
    </div>
    """
    
    # Enhanced column summary with better styling and persistence
    columns_summary_html = generate_columns_summary(analysis, dataset_info)
    
    # Selection info
    selection_info = ""
    if selected_columns:
        selection_info = f"""
        <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-4 mb-6">
            <h3 class="text-lg font-semibold text-yellow-800 mb-2">🎯 Custom Selection</h3>
            <p class="text-yellow-700">
                <strong>Selected Columns:</strong> {', '.join(selected_columns)}<br>
                <strong>Chart Types:</strong> {', '.join(chart_types) if chart_types else 'Auto-generated'}
            </p>
        </div>
        """
    
    # Available chart types info
    chart_types_info = """
    <div class="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
        <h3 class="text-lg font-semibold text-blue-800 mb-2">📊 Available Chart Types</h3>
        <div class="grid grid-cols-2 md:grid-cols-4 gap-2 text-sm">
            <span class="bg-white px-2 py-1 rounded border">histogram</span>
            <span class="bg-white px-2 py-1 rounded border">bar</span>
            <span class="bg-white px-2 py-1 rounded border">scatter</span>
            <span class="bg-white px-2 py-1 rounded border">box</span>
            <span class="bg-white px-2 py-1 rounded border">line</span>
            <span class="bg-white px-2 py-1 rounded border">area</span>
            <span class="bg-white px-2 py-1 rounded border">pie</span>
            <span class="bg-white px-2 py-1 rounded border">heatmap</span>
            <span class="bg-white px-2 py-1 rounded border">violin</span>
            <span class="bg-white px-2 py-1 rounded border">density_contour</span>
            <span class="bg-white px-2 py-1 rounded border">bubble</span>
            <span class="bg-white px-2 py-1 rounded border">treemap</span>
        </div>
    </div>
    """
    
    return f"""
<!DOCTYPE html>
<html>
<head>
    <title>Advanced Data Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .chart-container {{
            background: white;
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 24px;
            box-shadow: 0 8px 25px rgba(0,0,0,0.1);
            border: 1px solid #e5e7eb;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        .chart-container:hover {{
            transform: translateY(-2px);
            box-shadow: 0 12px 35px rgba(0,0,0,0.15);
        }}
        .stat-card {{
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        .stat-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 8px 15px rgba(0,0,0,0.1);
        }}
        .vertical-layout {{
            display: flex;
            flex-direction: column;
            gap: 24px;
        }}
        .column-item {{
            transition: all 0.2s ease;
            border-left: 4px solid transparent;
        }}
        .column-item:hover {{
            border-left-color: #3B82F6;
            background-color: #f8fafc;
            transform: translateX(4px);
        }}
        .numeric-badge {{
            background-color: #dbeafe;
            color: #1e40af;
            border: 1px solid #93c5fd;
        }}
        .categorical-badge {{
            background-color: #dcfce7;
            color: #166534;
            border: 1px solid #86efac;
        }}
        .chart-image {{
            width: 100%;
            height: 500px;
            object-fit: contain;
            border-radius: 12px;
            border: 1px solid #e5e7eb;
        }}
        .stats-container {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 12px;
            padding: 20px;
        }}
        .data-overview {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            border: 1px solid #e5e7eb;
        }}
    </style>
</head>
<body class="bg-gradient-to-br from-gray-50 to-gray-100 min-h-screen">
    <div class="container mx-auto px-4 py-8">
        <!-- Header -->
        <div class="mb-8 text-center">
            <h1 class="text-4xl font-bold text-gray-800 mb-2">📊 Advanced Data Dashboard</h1>
            <p class="text-gray-600 text-lg">Dataset: <span class="font-semibold text-blue-600">{dataset_info['name']}</span></p>
        </div>
        
        <!-- Dataset Overview -->
        {overview_html}
        
        <!-- Selection Info -->
        {selection_info}
        
        <!-- Chart Types Info -->
        {chart_types_info}
        
        <!-- Column Information - Always visible -->
        <div class="bg-white rounded-xl shadow-lg p-6 mb-8">
            <h2 class="text-2xl font-bold text-gray-800 mb-6 text-center">🗂️ Dataset Columns ({len(dataset_info['columns'])} columns)</h2>
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {columns_summary_html}
            </div>
        </div>
        
        <!-- Main Visualizations in Vertical Layout -->
        <div class="mb-8">
            <h2 class="text-3xl font-bold text-gray-800 mb-8 text-center">📈 Data Insights & Visualizations</h2>
            <div class="vertical-layout">
                {charts_html}
            </div>
        </div>
        
        <!-- Data Summary -->
        <div class="bg-white rounded-xl shadow-lg p-8 mb-8">
            <h2 class="text-2xl font-bold text-gray-800 mb-6 text-center">📋 Dataset Summary</h2>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div>
                    <h3 class="text-xl font-semibold text-gray-700 mb-4">📊 Data Quality</h3>
                    <div class="space-y-4">
                        <div class="flex justify-between items-center p-3 bg-gradient-to-r from-green-50 to-blue-50 rounded-lg">
                            <span class="text-gray-700 font-medium">Data Completeness</span>
                            <span class="font-bold text-green-600 text-lg">{calculate_completeness(analysis, dataset_info):.1f}%</span>
                        </div>
                        <div class="flex justify-between items-center p-3 bg-gradient-to-r from-blue-50 to-purple-50 rounded-lg">
                            <span class="text-gray-700 font-medium">Data Uniqueness</span>
                            <span class="font-bold text-blue-600 text-lg">{calculate_uniqueness(analysis, dataset_info):.1f}%</span>
                        </div>
                        <div class="flex justify-between items-center p-3 bg-gradient-to-r from-purple-50 to-pink-50 rounded-lg">
                            <span class="text-gray-700 font-medium">Numeric Columns</span>
                            <span class="font-bold text-purple-600 text-lg">{count_numeric_columns(analysis)}</span>
                        </div>
                        <div class="flex justify-between items-center p-3 bg-gradient-to-r from-orange-50 to-red-50 rounded-lg">
                            <span class="text-gray-700 font-medium">Categorical Columns</span>
                            <span class="font-bold text-orange-600 text-lg">{count_categorical_columns(analysis)}</span>
                        </div>
                    </div>
                </div>
                <div>
                    <h3 class="text-xl font-semibold text-gray-700 mb-4">🚀 Quick Stats</h3>
                    <div class="space-y-4">
                        <div class="flex justify-between items-center p-3 bg-gradient-to-r from-gray-50 to-blue-50 rounded-lg">
                            <span class="text-gray-700 font-medium">Total Records</span>
                            <span class="font-bold text-gray-800 text-lg">{dataset_info['row_count']:,}</span>
                        </div>
                        <div class="flex justify-between items-center p-3 bg-gradient-to-r from-blue-50 to-green-50 rounded-lg">
                            <span class="text-gray-700 font-medium">Total Features</span>
                            <span class="font-bold text-gray-800 text-lg">{dataset_info['column_count']}</span>
                        </div>
                        <div class="flex justify-between items-center p-3 bg-gradient-to-r from-green-50 to-purple-50 rounded-lg">
                            <span class="text-gray-700 font-medium">Memory Usage</span>
                            <span class="font-bold text-gray-800 text-lg">{calculate_memory_usage(df)}</span>
                        </div>
                        <div class="flex justify-between items-center p-3 bg-gradient-to-r from-purple-50 to-orange-50 rounded-lg">
                            <span class="text-gray-700 font-medium">Data Types</span>
                            <span class="font-bold text-gray-800 text-lg">{len(set(df.dtypes))}</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
"""

def create_vertical_visualizations(df: pd.DataFrame, analysis: dict, selected_columns: List[str] = None, chart_types: List[str] = None) -> str:
    """Create visualizations in vertical layout with manual selection support"""
    charts = []
    
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    
    print(f"DEBUG: Creating visualizations - Numeric cols: {numeric_cols}, Categorical cols: {categorical_cols}")
    
    # If manual selection is provided, use that
    if selected_columns and chart_types:
        i = 0
        while i < len(selected_columns):
            col = selected_columns[i]
            chart_type = chart_types[i] if i < len(chart_types) else 'auto'
            
            if col in df.columns:
                print(f"DEBUG: Creating manual chart {i+1}: {chart_type} for {col}")
                
                # Handle different chart types
                if chart_type == 'scatter':
                    # For scatter plot, we need two numeric columns
                    if i + 1 < len(selected_columns):
                        col2 = selected_columns[i + 1]
                        if col2 in numeric_cols and col in numeric_cols:
                            fig = create_beautiful_chart('scatter', df, col, col2, title=f"{col} vs {col2}")
                            title = f"🎯 {col} vs {col2}"
                            i += 1  # Skip next column since we used it for scatter
                        else:
                            # Fallback to histogram if second column is not numeric or not available
                            if col in numeric_cols:
                                fig = create_beautiful_chart('histogram', df, col, title=f"Distribution of {col}")
                                title = f"📊 Distribution of {col}"
                            else:
                                fig = create_beautiful_chart('bar', df, col, title=f"Top {col} Categories")
                                title = f"📈 Top {col} Categories"
                    else:
                        # Not enough columns for scatter, fallback
                        if col in numeric_cols:
                            fig = create_beautiful_chart('histogram', df, col, title=f"Distribution of {col}")
                            title = f"📊 Distribution of {col}"
                        else:
                            fig = create_beautiful_chart('bar', df, col, title=f"Top {col} Categories")
                            title = f"📈 Top {col} Categories"
                
                elif chart_type == 'line' and col in numeric_cols:
                    # For line chart, we need a second numeric column or use index
                    if i + 1 < len(selected_columns) and selected_columns[i + 1] in numeric_cols:
                        col2 = selected_columns[i + 1]
                        fig = create_beautiful_chart('line', df, col, col2, title=f"{col} vs {col2}")
                        title = f"📈 {col} vs {col2}"
                        i += 1
                    else:
                        # Use index as x-axis
                        temp_df = df.reset_index()
                        fig = create_beautiful_chart('line', temp_df, 'index', col, title=f"{col} Trend")
                        title = f"📈 {col} Trend"
                
                elif chart_type == 'area' and col in numeric_cols:
                    # Similar to line chart
                    if i + 1 < len(selected_columns) and selected_columns[i + 1] in numeric_cols:
                        col2 = selected_columns[i + 1]
                        fig = create_beautiful_chart('area', df, col, col2, title=f"{col} vs {col2} Area")
                        title = f"🟨 {col} vs {col2} Area"
                        i += 1
                    else:
                        temp_df = df.reset_index()
                        fig = create_beautiful_chart('area', temp_df, 'index', col, title=f"{col} Area")
                        title = f"🟨 {col} Area"
                
                elif chart_type == 'pie' and col in categorical_cols:
                    fig = create_beautiful_chart('pie', df, col, title=f"{col} Distribution")
                    title = f"🥧 {col} Distribution"
                
                elif chart_type == 'heatmap':
                    fig = create_beautiful_chart('heatmap', df, col, title="Correlation Heatmap")
                    title = "🔥 Correlation Heatmap"
                    # Heatmap uses all numeric columns, so we don't need specific column
                
                elif chart_type == 'violin' and col in numeric_cols:
                    fig = create_beautiful_chart('violin', df, col, title=f"{col} Distribution")
                    title = f"🎻 {col} Distribution"
                
                elif chart_type == 'density_contour' and col in numeric_cols:
                    if i + 1 < len(selected_columns) and selected_columns[i + 1] in numeric_cols:
                        col2 = selected_columns[i + 1]
                        fig = create_beautiful_chart('density_contour', df, col, col2, title=f"{col} vs {col2} Density")
                        title = f"🌊 {col} vs {col2} Density"
                        i += 1
                    else:
                        fig = create_beautiful_chart('histogram', df, col, title=f"Distribution of {col}")
                        title = f"📊 Distribution of {col}"
                
                elif chart_type == 'bubble' and col in numeric_cols:
                    # Bubble chart needs three numeric columns
                    if i + 2 < len(selected_columns):
                        col2, col3 = selected_columns[i + 1], selected_columns[i + 2]
                        if all(c in numeric_cols for c in [col2, col3]):
                            fig = create_beautiful_chart('bubble', df, col, col2, col3, title=f"Bubble: {col}, {col2}, {col3}")
                            title = f"🫧 Bubble: {col}, {col2}, {col3}"
                            i += 2
                        else:
                            fig = create_beautiful_chart('scatter', df, col, col2, title=f"{col} vs {col2}")
                            title = f"🎯 {col} vs {col2}"
                            i += 1
                    else:
                        fig = create_beautiful_chart('scatter', df, col, numeric_cols[1] if len(numeric_cols) > 1 else col, 
                                                   title=f"{col} vs {numeric_cols[1] if len(numeric_cols) > 1 else 'Value'}")
                        title = f"🎯 {col} Scatter"
                
                elif chart_type == 'treemap' and col in categorical_cols:
                    if i + 1 < len(selected_columns) and selected_columns[i + 1] in numeric_cols:
                        col2 = selected_columns[i + 1]
                        fig = create_beautiful_chart('treemap', df, col, col2, title=f"{col} Treemap by {col2}")
                        title = f"🌳 {col} Treemap"
                        i += 1
                    else:
                        fig = create_beautiful_chart('bar', df, col, title=f"Top {col} Categories")
                        title = f"📈 Top {col} Categories"
                
                elif chart_type == 'histogram' and col in numeric_cols:
                    fig = create_beautiful_chart('histogram', df, col, title=f"Distribution of {col}")
                    title = f"📊 Distribution of {col}"
                
                elif chart_type == 'bar' and col in categorical_cols:
                    fig = create_beautiful_chart('bar', df, col, title=f"Top {col} Categories")
                    title = f"📈 Top {col} Categories"
                
                elif chart_type == 'box' and col in numeric_cols:
                    fig = create_beautiful_chart('box', df, col, title=f"Box Plot of {col}")
                    title = f"📦 Box Plot of {col}"
                
                else:
                    # Auto-detect best chart type
                    if col in numeric_cols:
                        fig = create_beautiful_chart('histogram', df, col, title=f"Distribution of {col}")
                        title = f"📊 Distribution of {col}"
                    elif col in categorical_cols:
                        fig = create_beautiful_chart('bar', df, col, title=f"Top {col} Categories")
                        title = f"📈 Top {col} Categories"
                    else:
                        # Skip if column type not supported
                        i += 1
                        continue
                
                # Add chart to output if created successfully
                if fig:
                    img_data = plotly_to_image(fig)
                    if img_data:
                        chart_html = f"""
                        <div class="chart-container">
                            <h3 class="text-xl font-bold text-gray-800 mb-4">{title}</h3>
                            <img src="{img_data}" alt="{title}" class="chart-image">
                            <div class="mt-4 text-sm text-gray-600">
                                <p><strong>Chart Type:</strong> {chart_type}</p>
                                <p><strong>Data Column:</strong> {col}</p>
                            </div>
                        </div>
                        """
                        charts.append(chart_html)
                    else:
                        charts.append(create_empty_chart(f"Could not generate {chart_type} for {col}", f"chart{i}", title))
                else:
                    charts.append(create_empty_chart(f"Could not create {chart_type} for {col}", f"chart{i}", title))
            else:
                charts.append(create_empty_chart(f"Column '{col}' not found", f"chart{i}", "Missing Column"))
            
            i += 1
    
    else:
        # Auto-generate a variety of chart types based on available data
        print("DEBUG: Auto-generating diverse chart types")
        
        # 1. Correlation Heatmap (if enough numeric columns)
        if len(numeric_cols) >= 2:
            fig = create_beautiful_chart('heatmap', df, numeric_cols[0], title="Correlation Heatmap")
            if fig:
                img_data = plotly_to_image(fig)
                if img_data:
                    charts.append(f"""
                    <div class="chart-container">
                        <h3 class="text-xl font-bold text-gray-800 mb-4">🔥 Correlation Heatmap</h3>
                        <img src="{img_data}" alt="Correlation Heatmap" class="chart-image">
                        <div class="mt-4 text-sm text-gray-600">
                            <p><strong>Chart Type:</strong> heatmap</p>
                            <p><strong>Data Columns:</strong> All numeric columns</p>
                        </div>
                    </div>
                    """)
        
        # 2. Histogram for first numeric column
        if numeric_cols:
            col = numeric_cols[0]
            fig = create_beautiful_chart('histogram', df, col, title=f"Distribution of {col}")
            if fig:
                img_data = plotly_to_image(fig)
                if img_data:
                    charts.append(f"""
                    <div class="chart-container">
                        <h3 class="text-xl font-bold text-gray-800 mb-4">📊 Distribution of {col}</h3>
                        <img src="{img_data}" alt="Histogram" class="chart-image">
                        <div class="mt-4 text-sm text-gray-600">
                            <p><strong>Chart Type:</strong> histogram</p>
                            <p><strong>Data Column:</strong> {col}</p>
                        </div>
                    </div>
                    """)
        
        # 3. Box plot for second numeric column (if available)
        if len(numeric_cols) >= 2:
            col = numeric_cols[1]
            fig = create_beautiful_chart('box', df, col, title=f"Box Plot of {col}")
            if fig:
                img_data = plotly_to_image(fig)
                if img_data:
                    charts.append(f"""
                    <div class="chart-container">
                        <h3 class="text-xl font-bold text-gray-800 mb-4">📦 Box Plot of {col}</h3>
                        <img src="{img_data}" alt="Box Plot" class="chart-image">
                        <div class="mt-4 text-sm text-gray-600">
                            <p><strong>Chart Type:</strong> box</p>
                            <p><strong>Data Column:</strong> {col}</p>
                        </div>
                    </div>
                    """)
        
        # 4. Violin plot for third numeric column (if available)
        if len(numeric_cols) >= 3:
            col = numeric_cols[2]
            fig = create_beautiful_chart('violin', df, col, title=f"Violin Plot of {col}")
            if fig:
                img_data = plotly_to_image(fig)
                if img_data:
                    charts.append(f"""
                    <div class="chart-container">
                        <h3 class="text-xl font-bold text-gray-800 mb-4">🎻 Violin Plot of {col}</h3>
                        <img src="{img_data}" alt="Violin Plot" class="chart-image">
                        <div class="mt-4 text-sm text-gray-600">
                            <p><strong>Chart Type:</strong> violin</p>
                            <p><strong>Data Column:</strong> {col}</p>
                        </div>
                    </div>
                    """)
        
        # 5. Scatter plot for first two numeric columns
        if len(numeric_cols) >= 2:
            col1, col2 = numeric_cols[0], numeric_cols[1]
            fig = create_beautiful_chart('scatter', df, col1, col2, title=f"{col1} vs {col2}")
            if fig:
                img_data = plotly_to_image(fig)
                if img_data:
                    charts.append(f"""
                    <div class="chart-container">
                        <h3 class="text-xl font-bold text-gray-800 mb-4">🎯 {col1} vs {col2}</h3>
                        <img src="{img_data}" alt="Scatter Plot" class="chart-image">
                        <div class="mt-4 text-sm text-gray-600">
                            <p><strong>Chart Type:</strong> scatter</p>
                            <p><strong>Data Columns:</strong> {col1}, {col2}</p>
                        </div>
                    </div>
                    """)
        
        # 6. Density contour for first two numeric columns
        if len(numeric_cols) >= 2:
            col1, col2 = numeric_cols[0], numeric_cols[1]
            fig = create_beautiful_chart('density_contour', df, col1, col2, title=f"{col1} vs {col2} Density")
            if fig:
                img_data = plotly_to_image(fig)
                if img_data:
                    charts.append(f"""
                    <div class="chart-container">
                        <h3 class="text-xl font-bold text-gray-800 mb-4">🌊 {col1} vs {col2} Density</h3>
                        <img src="{img_data}" alt="Density Contour" class="chart-image">
                        <div class="mt-4 text-sm text-gray-600">
                            <p><strong>Chart Type:</strong> density_contour</p>
                            <p><strong>Data Columns:</strong> {col1}, {col2}</p>
                        </div>
                    </div>
                    """)
        
        # 7. Bar chart for first categorical column
        if categorical_cols:
            col = categorical_cols[0]
            fig = create_beautiful_chart('bar', df, col, title=f"Top {col} Categories")
            if fig:
                img_data = plotly_to_image(fig)
                if img_data:
                    charts.append(f"""
                    <div class="chart-container">
                        <h3 class="text-xl font-bold text-gray-800 mb-4">📈 Top {col} Categories</h3>
                        <img src="{img_data}" alt="Bar Chart" class="chart-image">
                        <div class="mt-4 text-sm text-gray-600">
                            <p><strong>Chart Type:</strong> bar</p>
                            <p><strong>Data Column:</strong> {col}</p>
                        </div>
                    </div>
                    """)
        
        # 8. Pie chart for second categorical column (if available)
        if len(categorical_cols) >= 2:
            col = categorical_cols[1]
            fig = create_beautiful_chart('pie', df, col, title=f"{col} Distribution")
            if fig:
                img_data = plotly_to_image(fig)
                if img_data:
                    charts.append(f"""
                    <div class="chart-container">
                        <h3 class="text-xl font-bold text-gray-800 mb-4">🥧 {col} Distribution</h3>
                        <img src="{img_data}" alt="Pie Chart" class="chart-image">
                        <div class="mt-4 text-sm text-gray-600">
                            <p><strong>Chart Type:</strong> pie</p>
                            <p><strong>Data Column:</strong> {col}</p>
                        </div>
                    </div>
                    """)
        
        # 9. Line chart (using index as x-axis for first numeric column)
        if numeric_cols:
            col = numeric_cols[0]
            temp_df = df.reset_index()
            fig = create_beautiful_chart('line', temp_df, 'index', col, title=f"{col} Trend")
            if fig:
                img_data = plotly_to_image(fig)
                if img_data:
                    charts.append(f"""
                    <div class="chart-container">
                        <h3 class="text-xl font-bold text-gray-800 mb-4">📈 {col} Trend</h3>
                        <img src="{img_data}" alt="Line Chart" class="chart-image">
                        <div class="mt-4 text-sm text-gray-600">
                            <p><strong>Chart Type:</strong> line</p>
                            <p><strong>Data Column:</strong> {col}</p>
                        </div>
                    </div>
                    """)
        
        # 10. Area chart (using index as x-axis for second numeric column)
        if len(numeric_cols) >= 2:
            col = numeric_cols[1]
            temp_df = df.reset_index()
            fig = create_beautiful_chart('area', temp_df, 'index', col, title=f"{col} Area Chart")
            if fig:
                img_data = plotly_to_image(fig)
                if img_data:
                    charts.append(f"""
                    <div class="chart-container">
                        <h3 class="text-xl font-bold text-gray-800 mb-4">🟨 {col} Area Chart</h3>
                        <img src="{img_data}" alt="Area Chart" class="chart-image">
                        <div class="mt-4 text-sm text-gray-600">
                            <p><strong>Chart Type:</strong> area</p>
                            <p><strong>Data Column:</strong> {col}</p>
                        </div>
                    </div>
                    """)
    
    # If no charts were created, show message
    if not charts:
        charts = ["""
        <div class="chart-container text-center py-12">
            <div class="text-5xl mb-4">📊</div>
            <h3 class="text-xl font-bold text-gray-800 mb-4">No Visualizations Available</h3>
            <p class="text-gray-600 mb-4">The dataset may not contain suitable data for visualization.</p>
            <div class="text-sm text-gray-500">
                <p>Try uploading a dataset with:</p>
                <ul class="list-disc list-inside mt-2">
                    <li>Numeric columns for histograms, scatter plots, etc.</li>
                    <li>Categorical columns for bar charts, pie charts, etc.</li>
                    <li>At least 2 numeric columns for correlation heatmaps</li>
                </ul>
            </div>
        </div>
        """]
    
    return "\n".join(charts)

def generate_columns_summary(analysis: dict, dataset_info: dict) -> str:
    """Generate enhanced column summary with better styling"""
    columns_html = []
    
    # Debug: Print the structure to understand what we're working with
    print(f"DEBUG: dataset_info keys: {dataset_info.keys()}")
    print(f"DEBUG: dataset_info['columns'] type: {type(dataset_info.get('columns'))}")
    print(f"DEBUG: dataset_info['columns'] sample: {dataset_info.get('columns')[:3] if dataset_info.get('columns') and len(dataset_info.get('columns')) > 3 else dataset_info.get('columns')}")
    
    # Get columns from dataset_info - handle different possible structures
    columns = dataset_info.get('columns', [])
    
    if not columns:
        # If no columns in dataset_info, try to get from analysis
        columns = list(analysis.get('column_analysis', {}).keys())
        print(f"DEBUG: Using columns from analysis: {columns[:5]}...")
    
    for col_name in columns:
        try:
            # Handle case where columns is a list of strings
            if isinstance(col_name, str):
                column_name = col_name
                # Get column info from analysis
                col_info = analysis.get('column_analysis', {}).get(column_name, {})
                
            # Handle case where columns is a list of dictionaries
            elif isinstance(col_name, dict):
                column_name = col_name.get('name', 'Unknown')
                col_info = analysis.get('column_analysis', {}).get(column_name, {})
                # Merge with any info from the column dict
                if isinstance(col_name, dict):
                    col_info = {**col_name, **col_info}
            else:
                print(f"DEBUG: Unexpected column type: {type(col_name)} - {col_name}")
                continue
            
            # Determine if numeric based on available info
            is_numeric = any(key in col_info for key in ['mean', 'min', 'max', 'std']) or \
                        any(str(col_info.get('dtype', '')).lower().contains(word) for word in ['int', 'float', 'number']) if col_info else False
            
            badge_class = "numeric-badge" if is_numeric else "categorical-badge"
            badge_text = "🔢 Numeric" if is_numeric else "📝 Categorical"
            
            # Get basic column information
            dtype = col_info.get('dtype', 'Unknown')
            unique_count = col_info.get('unique_values', col_info.get('unique_count', 'N/A'))
            missing_count = col_info.get('missing_values', col_info.get('null_count', 'N/A'))
            
            # Build stats HTML
            stats_html = ""
            if is_numeric:
                stats_html = f"""
                <div class="text-xs text-gray-600 mt-2 space-y-1">
                    <div class="flex justify-between">
                        <span>Mean:</span>
                        <span class="font-medium">{col_info.get('mean', 'N/A'):.2f}</span>
                    </div>
                    <div class="flex justify-between">
                        <span>Range:</span>
                        <span class="font-medium">{col_info.get('min', 'N/A')} - {col_info.get('max', 'N/A')}</span>
                    </div>
                </div>
                """
            else:
                stats_html = f"""
                <div class="text-xs text-gray-600 mt-2 space-y-1">
                    <div class="flex justify-between">
                        <span>Top Value:</span>
                        <span class="font-medium">{str(col_info.get('top_value', 'N/A'))[:20]}{'...' if len(str(col_info.get('top_value', ''))) > 20 else ''}</span>
                    </div>
                    <div class="flex justify-between">
                        <span>Freq:</span>
                        <span class="font-medium">{col_info.get('top_frequency', 'N/A')}</span>
                    </div>
                </div>
                """ if col_info.get('top_value') else ""
            
            columns_html.append(f"""
            <div class="column-item bg-white border border-gray-200 rounded-lg p-4 hover:shadow-md">
                <div class="flex justify-between items-start mb-2">
                    <h4 class="font-semibold text-gray-800 truncate" title="{column_name}">{column_name}</h4>
                    <span class="{badge_class} text-xs font-medium px-2 py-1 rounded-full">{badge_text}</span>
                </div>
                <div class="text-sm text-gray-600 space-y-1">
                    <div class="flex justify-between">
                        <span>Type:</span>
                        <span class="font-medium">{dtype}</span>
                    </div>
                    <div class="flex justify-between">
                        <span>Unique:</span>
                        <span class="font-medium">{unique_count}</span>
                    </div>
                    <div class="flex justify-between">
                        <span>Missing:</span>
                        <span class="font-medium">{missing_count}</span>
                    </div>
                </div>
                {stats_html}
            </div>
            """)
            
        except Exception as e:
            print(f"DEBUG: Error processing column {col_name}: {e}")
            # Create a basic column item as fallback
            columns_html.append(f"""
            <div class="column-item bg-white border border-gray-200 rounded-lg p-4">
                <div class="flex justify-between items-start mb-2">
                    <h4 class="font-semibold text-gray-800 truncate" title="{col_name}">{col_name}</h4>
                    <span class="categorical-badge text-xs font-medium px-2 py-1 rounded-full">📝 Unknown</span>
                </div>
                <div class="text-sm text-gray-600">
                    <div>Error loading column details</div>
                </div>
            </div>
            """)
    
    # If still no columns, create a fallback
    if not columns_html:
        columns_html = ["""
        <div class="column-item bg-white border border-gray-200 rounded-lg p-4 text-center">
            <div class="text-gray-500">No column information available</div>
        </div>
        """]
    
    return "\n".join(columns_html)

def calculate_completeness(analysis: dict, dataset_info: dict) -> float:
    """Calculate data completeness percentage"""
    try:
        total_cells = dataset_info.get('row_count', 0) * dataset_info.get('column_count', 0)
        missing_values = analysis.get('basic_stats', {}).get('missing_values', 0)
        if total_cells == 0:
            return 0.0
        return ((total_cells - missing_values) / total_cells) * 100
    except Exception as e:
        print(f"DEBUG: Error in calculate_completeness: {e}")
        return 0.0

def calculate_uniqueness(analysis: dict, dataset_info: dict) -> float:
    """Calculate data uniqueness percentage"""
    try:
        total_rows = dataset_info.get('row_count', 0)
        duplicate_rows = analysis.get('basic_stats', {}).get('duplicate_rows', 0)
        if total_rows == 0:
            return 0.0
        return ((total_rows - duplicate_rows) / total_rows) * 100
    except Exception as e:
        print(f"DEBUG: Error in calculate_uniqueness: {e}")
        return 0.0

def count_numeric_columns(analysis: dict) -> int:
    """Count numeric columns"""
    try:
        count = 0
        for col_info in analysis.get('column_analysis', {}).values():
            if any(key in col_info for key in ['mean', 'min', 'max', 'std']):
                count += 1
        return count
    except Exception as e:
        print(f"DEBUG: Error in count_numeric_columns: {e}")
        return 0

def count_categorical_columns(analysis: dict) -> int:
    """Count categorical columns"""
    try:
        total_columns = len(analysis.get('column_analysis', {}))
        numeric_columns = count_numeric_columns(analysis)
        return total_columns - numeric_columns
    except Exception as e:
        print(f"DEBUG: Error in count_categorical_columns: {e}")
        return 0

def calculate_memory_usage(df: pd.DataFrame) -> str:
    """Calculate memory usage of DataFrame"""
    memory_bytes = df.memory_usage(deep=True).sum()
    if memory_bytes < 1024:
        return f"{memory_bytes} B"
    elif memory_bytes < 1024**2:
        return f"{memory_bytes / 1024:.1f} KB"
    elif memory_bytes < 1024**3:
        return f"{memory_bytes / (1024**2):.1f} MB"
    else:
        return f"{memory_bytes / (1024**3):.1f} GB"

def count_charts_in_html(html_content: str) -> int:
    """Count the number of charts in the HTML content"""
    return html_content.count('chart-container')
