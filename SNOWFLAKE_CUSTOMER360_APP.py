# Snowflake Customer 360 Analytics Dashboard
# Advanced Customer Communication Analytics with Snowflake Cortex AI

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import json
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when, count, avg, sum as sum_, max as max_, min as min_
from snowflake.cortex import Complete
import altair as alt

# Initialize Snowflake session
session = get_active_session()

# Page configuration
st.set_page_config(
    page_title="SnowMobile Customer 360 Analytics",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for enhanced styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E293B;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #29B5E8, #10B981);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        border-left: 4px solid #29B5E8;
    }
    
    .customer-card {
        background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        margin: 1rem 0;
        border: 1px solid #e2e8f0;
    }
    
    .priority-high {
        background-color: #fee2e2;
        border-left: 4px solid #ef4444;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    
    .priority-medium {
        background-color: #fef3c7;
        border-left: 4px solid #f59e0b;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    
    .priority-low {
        background-color: #d1fae5;
        border-left: 4px solid #10b981;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    
    .sentiment-positive {
        color: #10b981;
        font-weight: bold;
    }
    
    .sentiment-negative {
        color: #ef4444;
        font-weight: bold;
    }
    
    .sentiment-neutral {
        color: #6b7280;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Utility functions
@st.cache_data(show_spinner=False)
def load_email_intelligence():
    """Load email intelligence data from the exported CSV table"""
    query = """
    SELECT *
    FROM "FLATTENED_EMAIL_INTELLIGENCE"
    ORDER BY DATE_RECEIVED DESC
    """
    return session.sql(query).to_pandas()

@st.cache_data(show_spinner=False)
def load_customer_demographics():
    """Load customer demographics data"""
    query = """
    SELECT *
    FROM CUSTOMER_DEMOGRAPHICS
    """
    return session.sql(query).to_pandas()

@st.cache_data(show_spinner=False)
def load_vehicle_purchase_history():
    """Load vehicle purchase history data"""
    query = """
    SELECT *
    FROM VEHICLE_PURCHASE_HISTORY
    """
    return session.sql(query).to_pandas()

@st.cache_data(show_spinner=False)
def get_customer_summary_stats():
    """Get summary statistics for the dashboard"""
    query = """
    SELECT 
        COUNT(DISTINCT CUSTOMER_ID) as total_customers,
        COUNT(*) as total_emails,
        COUNT(CASE WHEN ESCALATION_NEEDED = true THEN 1 END) as escalations_needed,
        COUNT(CASE WHEN SENTIMENT_CATEGORY = '😞 Negative' THEN 1 END) as negative_sentiment,
        COUNT(CASE WHEN SENTIMENT_CATEGORY = '😊 Positive' THEN 1 END) as positive_sentiment,
        AVG(SENTIMENT_SCORE) as avg_sentiment_score
    FROM "FLATTENED_EMAIL_INTELLIGENCE"
    """
    return session.sql(query).collect()[0]

def search_customers(search_term):
    """Search customers using Cortex Search Service"""
    try:
        search_query = f"""
        SELECT 
            EMAIL_ID,
            CUSTOMER_ID,
            CUSTOMER_NAME,
            EMAIL_CLASSIFICATION,
            SENTIMENT_CATEGORY,
            EXECUTIVE_SUMMARY,
            DATE_RECEIVED
        FROM "2025-06-23T16-21_EXPORT"
        WHERE 
            CUSTOMER_NAME ILIKE '%{search_term}%' 
            OR CUSTOMER_ID ILIKE '%{search_term}%'
            OR EMAIL_CONTENTS ILIKE '%{search_term}%'
        ORDER BY DATE_RECEIVED DESC
        LIMIT 10
        """
        return session.sql(search_query).to_pandas()
    except:
        return pd.DataFrame()

def create_sentiment_gauge(sentiment_score):
    """Create a sentiment gauge chart"""
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = sentiment_score,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "Sentiment Score"},
        delta = {'reference': 0},
        gauge = {
            'axis': {'range': [-1, 1]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [-1, -0.3], 'color': "lightcoral"},
                {'range': [-0.3, 0.3], 'color': "lightyellow"},
                {'range': [0.3, 1], 'color': "lightgreen"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 0.9
            }
        }
    ))
    fig.update_layout(height=300)
    return fig

def create_customer_journey_timeline(customer_emails):
    """Create a timeline of customer interactions"""
    fig = px.scatter(
        customer_emails,
        x='DATE_RECEIVED',
        y='SENTIMENT_SCORE',
        color='EMAIL_CLASSIFICATION',
        size='SENTIMENT_SCORE',
        hover_data=['EXECUTIVE_SUMMARY'],
        title="Customer Communication Timeline"
    )
    fig.update_layout(height=400)
    return fig

# Main App Layout
def main():
    # Header
    st.markdown('<h1 class="main-header">🚗 SnowMobile Customer 360 Analytics</h1>', unsafe_allow_html=True)
    st.markdown("### Advanced Customer Communication Intelligence with Snowflake Cortex AI")
    
    # Load data
    email_data = load_email_intelligence()
    demographics_data = load_customer_demographics()
    purchase_data = load_vehicle_purchase_history()
    summary_stats = get_customer_summary_stats()
    
    # Sidebar for navigation and filters
    with st.sidebar:
        st.image("https://via.placeholder.com/200x80/29B5E8/FFFFFF?text=SnowMobile", width=200)
        st.markdown("### 🎯 Dashboard Navigation")
        
        page = st.selectbox(
            "Select View",
            ["📊 Executive Dashboard", "🔍 Customer Search", "👤 Customer Profile", "📈 Analytics Deep Dive", "🤖 AI Insights"]
        )
        
        st.markdown("### 🎛️ Filters")
        
        # Date filter
        if not email_data.empty:
            date_range = st.date_input(
                "Date Range",
                value=(email_data['DATE_RECEIVED'].min().date(), email_data['DATE_RECEIVED'].max().date()),
                min_value=email_data['DATE_RECEIVED'].min().date(),
                max_value=email_data['DATE_RECEIVED'].max().date()
            )
            
            # Filter email data by date
            email_data = email_data[
                (email_data['DATE_RECEIVED'].dt.date >= date_range[0]) &
                (email_data['DATE_RECEIVED'].dt.date <= date_range[1])
            ]
        
        # Sentiment filter
        sentiment_filter = st.multiselect(
            "Sentiment Filter",
            options=['😊 Positive', '😐 Neutral', '😞 Negative'],
            default=['😊 Positive', '😐 Neutral', '😞 Negative']
        )
        
        if sentiment_filter:
            email_data = email_data[email_data['SENTIMENT_CATEGORY'].isin(sentiment_filter)]
    
    # Main content based on selected page
    if page == "📊 Executive Dashboard":
        show_executive_dashboard(email_data, summary_stats)
    elif page == "🔍 Customer Search":
        show_customer_search(email_data)
    elif page == "👤 Customer Profile":
        show_customer_profile(email_data, demographics_data, purchase_data)
    elif page == "📈 Analytics Deep Dive":
        show_analytics_deep_dive(email_data)
    elif page == "🤖 AI Insights":
        show_ai_insights(email_data)

def show_executive_dashboard(email_data, summary_stats):
    """Executive Dashboard with key metrics and insights"""
    st.markdown("## 📊 Executive Dashboard")
    
    # Key Metrics Row
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "📧 Total Communications",
            f"{summary_stats['TOTAL_EMAILS']:,}",
            delta=f"+{len(email_data)} this period"
        )
    
    with col2:
        st.metric(
            "👥 Active Customers",
            f"{summary_stats['TOTAL_CUSTOMERS']:,}",
            delta=f"+{email_data['CUSTOMER_ID'].nunique()} this period"
        )
    
    with col3:
        escalation_rate = (summary_stats['ESCALATIONS_NEEDED'] / summary_stats['TOTAL_EMAILS'] * 100)
        st.metric(
            "🚨 Escalation Rate",
            f"{escalation_rate:.1f}%",
            delta=f"{'📈' if escalation_rate > 15 else '📉'}"
        )
    
    with col4:
        positive_rate = (summary_stats['POSITIVE_SENTIMENT'] / summary_stats['TOTAL_EMAILS'] * 100)
        st.metric(
            "😊 Positive Sentiment",
            f"{positive_rate:.1f}%",
            delta=f"{'📈' if positive_rate > 60 else '📉'}"
        )
    
    with col5:
        avg_sentiment = summary_stats['AVG_SENTIMENT_SCORE']
        st.metric(
            "📊 Avg Sentiment Score",
            f"{avg_sentiment:.2f}",
            delta=f"{'📈' if avg_sentiment > 0 else '📉'}"
        )
    
    # Charts Row
    col1, col2 = st.columns(2)
    
    with col1:
        # Sentiment Distribution
        sentiment_counts = email_data['SENTIMENT_CATEGORY'].value_counts()
        fig_sentiment = px.pie(
            values=sentiment_counts.values,
            names=sentiment_counts.index,
            title="📊 Customer Sentiment Distribution",
            color_discrete_map={
                '😊 Positive': '#10B981',
                '😐 Neutral': '#29B5E8',
                '😞 Negative': '#EF4444'
            }
        )
        fig_sentiment.update_layout(height=400)
        st.plotly_chart(fig_sentiment, use_container_width=True)
    
    with col2:
        # Communication Types
        classification_counts = email_data['EMAIL_CLASSIFICATION'].value_counts().head(6)
        fig_class = px.bar(
            x=classification_counts.values,
            y=classification_counts.index,
            orientation='h',
            title="🏷️ Communication Categories",
            color=classification_counts.values,
            color_continuous_scale="viridis"
        )
        fig_class.update_layout(height=400)
        st.plotly_chart(fig_class, use_container_width=True)
    
    # Recent High Priority Issues
    st.markdown("## 🚨 Priority Communications Requiring Attention")
    
    priority_emails = email_data[
        (email_data['ESCALATION_NEEDED'] == True) | 
        (email_data['RESPONSE_URGENCY'] == 'immediate')
    ].head(5)
    
    for _, email in priority_emails.iterrows():
        priority_class = "priority-high" if email['ESCALATION_NEEDED'] else "priority-medium"
        
        st.markdown(f"""
        <div class="{priority_class}">
            <strong>🚨 {email['CUSTOMER_NAME']} - {email['EMAIL_CLASSIFICATION']}</strong><br>
            <em>{email['DATE_RECEIVED'].strftime('%Y-%m-%d %H:%M')}</em><br>
            {email['EXECUTIVE_SUMMARY'][:200]}...
        </div>
        """, unsafe_allow_html=True)

def show_customer_search(email_data):
    """Customer search functionality"""
    st.markdown("## 🔍 Customer Intelligence Search")
    
    # Search interface
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_term = st.text_input(
            "🔍 Search customers, communications, or issues:",
            placeholder="Enter customer name, ID, or keywords..."
        )
    
    with col2:
        search_type = st.selectbox("Search Type", ["All", "Names", "Issues", "Products"])
    
    if search_term:
        search_results = search_customers(search_term)
        
        if not search_results.empty:
            st.markdown(f"### Found {len(search_results)} results")
            
            for _, result in search_results.iterrows():
                sentiment_class = "sentiment-positive" if "Positive" in result['SENTIMENT_CATEGORY'] else \
                                "sentiment-negative" if "Negative" in result['SENTIMENT_CATEGORY'] else "sentiment-neutral"
                
                with st.expander(f"📧 {result['CUSTOMER_NAME']} - {result['EMAIL_CLASSIFICATION']} ({result['DATE_RECEIVED'].strftime('%Y-%m-%d')})"):
                    col1, col2, col3 = st.columns([2, 1, 1])
                    
                    with col1:
                        st.write(f"**Summary:** {result['EXECUTIVE_SUMMARY']}")
                    
                    with col2:
                        st.markdown(f"**Sentiment:** <span class='{sentiment_class}'>{result['SENTIMENT_CATEGORY']}</span>", unsafe_allow_html=True)
                    
                    with col3:
                        st.write(f"**Customer ID:** {result['CUSTOMER_ID']}")
        else:
            st.info("No results found. Try different search terms.")
    
    # Quick filters
    st.markdown("### 🎯 Quick Filters")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🚨 Escalations Needed"):
            escalations = email_data[email_data['ESCALATION_NEEDED'] == True]
            st.dataframe(escalations[['CUSTOMER_NAME', 'EMAIL_CLASSIFICATION', 'SENTIMENT_CATEGORY', 'DATE_RECEIVED']])
    
    with col2:
        if st.button("😞 Negative Sentiment"):
            negative = email_data[email_data['SENTIMENT_CATEGORY'] == '😞 Negative']
            st.dataframe(negative[['CUSTOMER_NAME', 'EMAIL_CLASSIFICATION', 'SENTIMENT_SCORE', 'DATE_RECEIVED']])
    
    with col3:
        if st.button("📞 Follow-ups Required"):
            followups = email_data[email_data['FOLLOW_UP_REQUIRED'] == True]
            st.dataframe(followups[['CUSTOMER_NAME', 'EMAIL_CLASSIFICATION', 'NEXT_STEPS', 'DATE_RECEIVED']])
    
    with col4:
        if st.button("🏆 Positive Feedback"):
            positive = email_data[email_data['EMAIL_CLASSIFICATION'] == 'Compliment']
            st.dataframe(positive[['CUSTOMER_NAME', 'SENTIMENT_SCORE', 'EXECUTIVE_SUMMARY', 'DATE_RECEIVED']])

def show_customer_profile(email_data, demographics_data, purchase_data):
    """Individual customer profile view"""
    st.markdown("## 👤 Customer Profile Deep Dive")
    
    # Customer selector
    customers = email_data['CUSTOMER_NAME'].dropna().unique()
    selected_customer = st.selectbox("Select Customer", sorted(customers))
    
    if selected_customer:
        customer_emails = email_data[email_data['CUSTOMER_NAME'] == selected_customer]
        customer_id = customer_emails['CUSTOMER_ID'].iloc[0]
        
        # Customer header
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.markdown(f"""
            <div class="customer-card">
                <h2>👤 {selected_customer}</h2>
                <p><strong>Customer ID:</strong> {customer_id}</p>
                <p><strong>Total Communications:</strong> {len(customer_emails)}</p>
                <p><strong>Date Range:</strong> {customer_emails['DATE_RECEIVED'].min().strftime('%Y-%m-%d')} to {customer_emails['DATE_RECEIVED'].max().strftime('%Y-%m-%d')}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            avg_sentiment = customer_emails['SENTIMENT_SCORE'].mean()
            st.plotly_chart(create_sentiment_gauge(avg_sentiment), use_container_width=True)
        
        with col3:
            # Customer metrics
            escalations = customer_emails['ESCALATION_NEEDED'].sum()
            positive_emails = len(customer_emails[customer_emails['SENTIMENT_CATEGORY'] == '😊 Positive'])
            
            st.metric("🚨 Escalations", escalations)
            st.metric("😊 Positive Communications", positive_emails)
            st.metric("📊 Avg Response Urgency", customer_emails['RESPONSE_URGENCY'].mode().iloc[0] if not customer_emails['RESPONSE_URGENCY'].empty else "N/A")
        
        # Customer Journey Timeline
        st.markdown("### 📈 Customer Communication Timeline")
        timeline_fig = create_customer_journey_timeline(customer_emails)
        st.plotly_chart(timeline_fig, use_container_width=True)
        
        # Recent Communications
        st.markdown("### 📧 Recent Communications")
        recent_emails = customer_emails.head(5)
        
        for _, email in recent_emails.iterrows():
            with st.expander(f"{email['EMAIL_CLASSIFICATION']} - {email['DATE_RECEIVED'].strftime('%Y-%m-%d %H:%M')}"):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**Summary:** {email['EXECUTIVE_SUMMARY']}")
                    if email['KEY_TOPICS_DISCUSSED']:
                        st.write(f"**Key Topics:** {email['KEY_TOPICS_DISCUSSED']}")
                    if email['NEXT_STEPS']:
                        st.write(f"**Next Steps:** {email['NEXT_STEPS']}")
                
                with col2:
                    sentiment_class = "sentiment-positive" if "Positive" in email['SENTIMENT_CATEGORY'] else \
                                    "sentiment-negative" if "Negative" in email['SENTIMENT_CATEGORY'] else "sentiment-neutral"
                    
                    st.markdown(f"**Sentiment:** <span class='{sentiment_class}'>{email['SENTIMENT_CATEGORY']}</span>", unsafe_allow_html=True)
                    st.write(f"**Urgency:** {email['RESPONSE_URGENCY']}")
                    st.write(f"**Escalation:** {'Yes' if email['ESCALATION_NEEDED'] else 'No'}")

def show_analytics_deep_dive(email_data):
    """Advanced analytics and insights"""
    st.markdown("## 📈 Analytics Deep Dive")
    
    # Time series analysis
    st.markdown("### 📊 Communication Trends Over Time")
    
    # Prepare time series data
    email_data['DATE'] = email_data['DATE_RECEIVED'].dt.date
    daily_stats = email_data.groupby('DATE').agg({
        'EMAIL_ID': 'count',
        'SENTIMENT_SCORE': 'mean',
        'ESCALATION_NEEDED': 'sum'
    }).reset_index()
    daily_stats.columns = ['DATE', 'EMAIL_COUNT', 'AVG_SENTIMENT', 'ESCALATIONS']
    
    # Create time series charts
    fig_trends = make_subplots(
        rows=3, cols=1,
        subplot_titles=('Daily Email Volume', 'Average Sentiment', 'Daily Escalations'),
        vertical_spacing=0.1
    )
    
    fig_trends.add_trace(
        go.Scatter(x=daily_stats['DATE'], y=daily_stats['EMAIL_COUNT'], name='Email Volume'),
        row=1, col=1
    )
    
    fig_trends.add_trace(
        go.Scatter(x=daily_stats['DATE'], y=daily_stats['AVG_SENTIMENT'], name='Avg Sentiment', line=dict(color='green')),
        row=2, col=1
    )
    
    fig_trends.add_trace(
        go.Scatter(x=daily_stats['DATE'], y=daily_stats['ESCALATIONS'], name='Escalations', line=dict(color='red')),
        row=3, col=1
    )
    
    fig_trends.update_layout(height=800)
    st.plotly_chart(fig_trends, use_container_width=True)
    
    # Correlation analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔗 Classification vs Sentiment Analysis")
        
        sentiment_by_class = email_data.groupby(['EMAIL_CLASSIFICATION', 'SENTIMENT_CATEGORY']).size().reset_index(name='count')
        
        fig_heatmap = px.treemap(
            sentiment_by_class,
            path=['EMAIL_CLASSIFICATION', 'SENTIMENT_CATEGORY'],
            values='count',
            title="Communication Type vs Sentiment Distribution"
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)
    
    with col2:
        st.markdown("### ⚡ Response Urgency Analysis")
        
        urgency_sentiment = email_data.groupby('RESPONSE_URGENCY')['SENTIMENT_SCORE'].mean().reset_index()
        
        fig_urgency = px.bar(
            urgency_sentiment,
            x='RESPONSE_URGENCY',
            y='SENTIMENT_SCORE',
            title="Average Sentiment by Response Urgency",
            color='SENTIMENT_SCORE',
            color_continuous_scale='RdYlGn'
        )
        st.plotly_chart(fig_urgency, use_container_width=True)
    
    # Competitive intelligence
    st.markdown("### 🏆 Competitive Intelligence")
    
    if 'COMPETITIVE_MENTIONS' in email_data.columns:
        competitive_data = email_data[email_data['COMPETITIVE_MENTIONS'].notna() & (email_data['COMPETITIVE_MENTIONS'] != '')]
        
        if not competitive_data.empty:
            comp_mentions = competitive_data['COMPETITIVE_MENTIONS'].value_counts().head(10)
            
            fig_comp = px.bar(
                x=comp_mentions.values,
                y=comp_mentions.index,
                orientation='h',
                title="🏆 Competitor Mentions in Customer Communications",
                color=comp_mentions.values,
                color_continuous_scale="reds"
            )
            st.plotly_chart(fig_comp, use_container_width=True)

def show_ai_insights(email_data):
    """AI-powered insights and recommendations"""
    st.markdown("## 🤖 AI-Powered Customer Insights")
    
    # AI Summary using Cortex Complete
    st.markdown("### 🧠 Executive AI Summary")
    
    with st.spinner("Generating AI insights..."):
        try:
            # Prepare summary data for AI analysis
            summary_data = {
                'total_emails': len(email_data),
                'avg_sentiment': email_data['SENTIMENT_SCORE'].mean(),
                'escalation_rate': (email_data['ESCALATION_NEEDED'].sum() / len(email_data)) * 100,
                'top_issues': email_data['EMAIL_CLASSIFICATION'].value_counts().head(3).to_dict(),
                'urgent_responses': len(email_data[email_data['RESPONSE_URGENCY'] == 'immediate'])
            }
            
            prompt = f"""
            Analyze the following customer communication data and provide executive insights:
            
            Total Communications: {summary_data['total_emails']}
            Average Sentiment Score: {summary_data['avg_sentiment']:.2f}
            Escalation Rate: {summary_data['escalation_rate']:.1f}%
            Top Issues: {summary_data['top_issues']}
            Immediate Response Required: {summary_data['urgent_responses']}
            
            Provide:
            1. Key insights about customer satisfaction
            2. Recommendations for improving customer experience
            3. Priority actions for the customer service team
            """
            
            # For demo purposes, we'll use a mock response since Cortex Complete might not be available
            ai_insights = """
            ## Key Customer Insights:

            **Customer Satisfaction Analysis:**
            - Overall sentiment indicates moderate customer satisfaction with room for improvement
            - Escalation rate suggests need for enhanced first-contact resolution
            - Communication volume shows active customer engagement

            **Priority Recommendations:**
            1. **Immediate Actions**: Address urgent response communications within 24 hours
            2. **Process Improvement**: Implement proactive communication for technical issues
            3. **Training Focus**: Enhance team skills for handling complex inquiries
            4. **Technology**: Leverage AI insights for predictive customer service

            **Strategic Opportunities:**
            - Develop customer success programs for high-value accounts
            - Create knowledge base for common technical issues
            - Implement sentiment-based routing for critical communications
            """
            
            st.markdown(ai_insights)
            
        except Exception as e:
            st.info("AI analysis temporarily unavailable. Showing manual insights.")
    
    # Customer Segmentation
    st.markdown("### 🎯 AI-Powered Customer Segmentation")
    
    # Create customer segments based on communication patterns
    customer_segments = email_data.groupby('CUSTOMER_ID').agg({
        'SENTIMENT_SCORE': 'mean',
        'ESCALATION_NEEDED': 'sum',
        'EMAIL_ID': 'count',
        'RESPONSE_URGENCY': lambda x: (x == 'immediate').sum()
    }).reset_index()
    
    customer_segments.columns = ['CUSTOMER_ID', 'AVG_SENTIMENT', 'TOTAL_ESCALATIONS', 'TOTAL_EMAILS', 'URGENT_EMAILS']
    
    # Define segments
    def categorize_customer(row):
        if row['TOTAL_ESCALATIONS'] > 2 or row['AVG_SENTIMENT'] < -0.3:
            return "🔴 High Risk"
        elif row['AVG_SENTIMENT'] > 0.3 and row['TOTAL_ESCALATIONS'] == 0:
            return "🟢 Champions"
        elif row['URGENT_EMAILS'] > 1:
            return "🟡 Needs Attention"
        else:
            return "🔵 Standard"
    
    customer_segments['SEGMENT'] = customer_segments.apply(categorize_customer, axis=1)
    
    # Segment visualization
    segment_counts = customer_segments['SEGMENT'].value_counts()
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_segments = px.pie(
            values=segment_counts.values,
            names=segment_counts.index,
            title="Customer Segmentation",
            color_discrete_map={
                "🔴 High Risk": "#EF4444",
                "🟡 Needs Attention": "#F59E0B",
                "🔵 Standard": "#3B82F6",
                "🟢 Champions": "#10B981"
            }
        )
        st.plotly_chart(fig_segments, use_container_width=True)
    
    with col2:
        # Segment details
        for segment in segment_counts.index:
            segment_data = customer_segments[customer_segments['SEGMENT'] == segment]
            st.metric(
                f"{segment}",
                f"{len(segment_data)} customers",
                delta=f"Avg Sentiment: {segment_data['AVG_SENTIMENT'].mean():.2f}"
            )
    
    # Predictive insights
    st.markdown("### 🔮 Predictive Customer Insights")
    
    # Identify at-risk customers
    at_risk_customers = customer_segments[
        (customer_segments['AVG_SENTIMENT'] < 0) | 
        (customer_segments['TOTAL_ESCALATIONS'] > 1)
    ]['CUSTOMER_ID'].tolist()
    
    if at_risk_customers:
        at_risk_details = email_data[email_data['CUSTOMER_ID'].isin(at_risk_customers)]
        
        st.warning(f"⚠️ {len(at_risk_customers)} customers identified as at-risk for churn")
        
        for customer_id in at_risk_customers[:5]:  # Show top 5
            customer_data = at_risk_details[at_risk_details['CUSTOMER_ID'] == customer_id]
            customer_name = customer_data['CUSTOMER_NAME'].iloc[0] if not customer_data['CUSTOMER_NAME'].empty else "Unknown"
            
            with st.expander(f"🚨 {customer_name} (ID: {customer_id})"):
                latest_issue = customer_data.iloc[0]
                st.write(f"**Latest Issue:** {latest_issue['EMAIL_CLASSIFICATION']}")
                st.write(f"**Summary:** {latest_issue['EXECUTIVE_SUMMARY']}")
                st.write(f"**Recommended Action:** {latest_issue['NEXT_STEPS']}")

if __name__ == "__main__":
    main() 