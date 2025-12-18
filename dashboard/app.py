"""Streamlit dashboard for MongoDB aggregated data visualization."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import pandas as pd
from decimal import Decimal
from typing import Any

from src.config import settings


# Page configuration
st.set_page_config(
    page_title="MongoDB Big Data Dashboard",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_resource
def get_mongo_client() -> MongoClient:
    """Create cached MongoDB connection."""
    return MongoClient(settings.mongodb_uri)


def decimal_to_float(obj: Any) -> Any:
    """Convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_float(item) for item in obj]
    return obj


def load_aggregation(collection_suffix: str) -> pd.DataFrame:
    """Load aggregated data from MongoDB."""
    client = get_mongo_client()
    db = client[settings.mongodb_database]
    collection_name = f"{settings.agg_collection}_{collection_suffix}"
    
    data = list(db[collection_name].find())
    
    if not data:
        return pd.DataFrame()
    
    # Convert Decimal to float
    data = [decimal_to_float(doc) for doc in data]
    
    df = pd.DataFrame(data)
    
    # Remove MongoDB _id field if it's ObjectId
    if '_id' in df.columns and 'id' in df.columns:
        df = df.drop('id', axis=1)
    
    return df


def format_currency(value: float) -> str:
    """Format value as currency."""
    return f"${value:,.2f}"


def format_number(value: int) -> str:
    """Format large numbers with commas."""
    return f"{value:,}"


# Main Dashboard
def main() -> None:
    """Main dashboard function."""
    
    st.title("MongoDB Sharded Cluster Analytics Dashboard")
    st.markdown("### Real-time insights from aggregated big data")
    
    # Sidebar
    with st.sidebar:
        st.header("Configuration")
        st.info(f"**Database:** {settings.mongodb_database}")
        st.info(f"**Host:** {settings.mongodb_host}:{settings.mongodb_port}")
        
        st.markdown("---")
        st.header("Available Views")
        view = st.radio(
            "Select Analysis View:",
            ["Overview", "Category Analysis", "Time Series", "Customer Insights", "Status Distribution"]
        )
        
        if st.button("Refresh Data"):
            st.cache_resource.clear()
            st.rerun()
    
    # Load data
    try:
        category_df = load_aggregation("category")
        month_df = load_aggregation("month")
        status_df = load_aggregation("status")
        user_df = load_aggregation("user")
        dow_df = load_aggregation("day_of_week")
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Make sure the pipeline has been executed and data is available.")
        return
    
    # Overview View
    if view == "Overview":
        st.header("Executive Summary")
        
        # KPI Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_revenue = category_df['total_revenue'].sum() if not category_df.empty else 0
        total_orders = category_df['total_orders'].sum() if not category_df.empty else 0
        unique_customers = user_df.shape[0] if not user_df.empty else 0
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        
        with col1:
            st.metric("Total Revenue", format_currency(total_revenue))
        with col2:
            st.metric("Total Orders", format_number(int(total_orders)))
        with col3:
            st.metric("Unique Customers", format_number(unique_customers))
        with col4:
            st.metric("Avg Order Value", format_currency(avg_order_value))
        
        st.markdown("---")
        
        # Two column layout for charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Revenue by Category")
            if not category_df.empty:
                fig = px.bar(
                    category_df.sort_values('total_revenue', ascending=False).head(10),
                    x='_id',
                    y='total_revenue',
                    title="Top 10 Categories by Revenue",
                    labels={'_id': 'Category', 'total_revenue': 'Revenue ($)'},
                    color='total_revenue',
                    color_continuous_scale='Blues'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Order Status Distribution")
            if not status_df.empty:
                fig = px.pie(
                    status_df,
                    values='total_orders',
                    names='_id',
                    title="Orders by Status",
                    hole=0.4
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Category Analysis View
    elif view == "Category Analysis":
        st.header("Category Performance Analysis")
        
        if category_df.empty:
            st.warning("No category data available")
            return
        
        # Category metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Revenue Distribution")
            fig = px.treemap(
                category_df,
                path=['_id'],
                values='total_revenue',
                title="Revenue by Category (Treemap)",
                color='total_revenue',
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Customer Engagement")
            fig = px.scatter(
                category_df,
                x='total_orders',
                y='unique_customers',
                size='total_revenue',
                color='avg_order_value',
                hover_data=['_id'],
                title="Orders vs Customers by Category",
                labels={
                    'total_orders': 'Total Orders',
                    'unique_customers': 'Unique Customers',
                    '_id': 'Category'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed table
        st.subheader("Category Details")
        display_df = category_df.copy()
        display_df['total_revenue'] = display_df['total_revenue'].apply(lambda x: f"${x:,.2f}")
        display_df['avg_order_value'] = display_df['avg_order_value'].apply(lambda x: f"${x:,.2f}")
        st.dataframe(display_df, use_container_width=True)
    
    # Time Series View
    elif view == "Time Series":
        st.header("Temporal Trends Analysis")
        
        if month_df.empty:
            st.warning("No temporal data available")
            return
        
        # Monthly trends
        st.subheader("Monthly Performance")
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=month_df['_id'],
            y=month_df['total_revenue'],
            name='Revenue',
            mode='lines+markers',
            line=dict(color='#2E86AB', width=3),
            yaxis='y'
        ))
        fig.add_trace(go.Scatter(
            x=month_df['_id'],
            y=month_df['total_orders'],
            name='Orders',
            mode='lines+markers',
            line=dict(color='#A23B72', width=3),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title='Revenue and Orders Over Time',
            xaxis=dict(title='Month'),
            yaxis=dict(title='Revenue ($)', side='left'),
            yaxis2=dict(title='Number of Orders', overlaying='y', side='right'),
            hovermode='x unified',
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Day of week analysis
        if not dow_df.empty:
            st.subheader("Day of Week Patterns")
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    dow_df,
                    x='_id',
                    y='total_orders',
                    title="Orders by Day of Week",
                    labels={'_id': 'Day', 'total_orders': 'Orders'},
                    color='total_orders',
                    color_continuous_scale='Teal'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(
                    dow_df,
                    x='_id',
                    y='avg_order_value',
                    title="Average Order Value by Day",
                    labels={'_id': 'Day', 'avg_order_value': 'Avg Order Value ($)'},
                    color='avg_order_value',
                    color_continuous_scale='Sunset'
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Customer Insights View
    elif view == "Customer Insights":
        st.header("Customer Behavior Analysis")
        
        if user_df.empty:
            st.warning("No customer data available")
            return
        
        # Top customers
        st.subheader("Top Customers by Revenue")
        
        top_customers = user_df.nlargest(20, 'total_revenue')
        
        fig = px.bar(
            top_customers,
            x='_id',
            y='total_revenue',
            title="Top 20 Customers",
            labels={'_id': 'Customer ID', 'total_revenue': 'Total Revenue ($)'},
            color='total_revenue',
            color_continuous_scale='Purples'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Customer segmentation
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Order Frequency Distribution")
            fig = px.histogram(
                user_df,
                x='total_orders',
                title="Distribution of Orders per Customer",
                labels={'total_orders': 'Number of Orders'},
                nbins=30
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Customer Lifetime Value")
            fig = px.box(
                user_df,
                y='total_revenue',
                title="Customer Revenue Distribution",
                labels={'total_revenue': 'Total Revenue ($)'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Status Distribution View
    elif view == "Status Distribution":
        st.header("Order Status Analysis")
        
        if status_df.empty:
            st.warning("No status data available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Orders by Status")
            fig = px.pie(
                status_df,
                values='total_orders',
                names='_id',
                title="Order Count by Status",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Revenue by Status")
            fig = px.pie(
                status_df,
                values='total_revenue',
                names='_id',
                title="Revenue by Status",
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Status details table
        st.subheader("Status Details")
        display_df = status_df.copy()
        display_df['total_revenue'] = display_df['total_revenue'].apply(lambda x: f"${x:,.2f}")
        display_df['avg_order_value'] = display_df['avg_order_value'].apply(lambda x: f"${x:,.2f}")
        st.dataframe(display_df, use_container_width=True)


if __name__ == "__main__":
    main()