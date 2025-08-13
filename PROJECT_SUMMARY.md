# 🎵 Lyrical Complexity Pipeline - Project Summary

## 🎯 What We Built

A **complete, production-ready data engineering pipeline** that analyzes the evolution of lyrical complexity in popular music over time. This project demonstrates **end-to-end ETL capabilities** and showcases the exact modern data stack that companies value.

## 🏗️ Complete Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Genius API    │    │   Data Pipeline  │    │   PostgreSQL    │
│   (Lyrics)      │───▶│   (ETL)          │───▶│   Database      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   Apache Airflow │
                       │   (Orchestration)│
                       └──────────────────┘
```

## ✨ Key Features Demonstrated

### 1. **Data Extraction (E)**

- **API Integration**: Uses Genius API to fetch lyrics for Billboard Hot 100 songs
- **Rate Limiting**: Implements respectful API usage with configurable delays
- **Error Handling**: Gracefully handles missing lyrics and API failures
- **Mock Data**: Includes realistic mock data for development/testing

### 2. **Data Transformation (T)**

- **Complexity Metrics**: Calculates Flesch-Kincaid readability scores, lexical diversity, word counts
- **Statistical Analysis**: Generates yearly and decade-level statistics
- **Data Cleaning**: Removes outliers and validates data quality
- **Trend Analysis**: Identifies complexity evolution patterns

### 3. **Data Loading (L)**

- **Database Design**: Optimized PostgreSQL schema with proper indexing
- **Data Quality**: Comprehensive validation and monitoring
- **Performance**: Efficient data loading with bulk operations
- **Metadata**: Tracks creation timestamps and data lineage

### 4. **Orchestration**

- **Apache Airflow**: Weekly scheduled pipeline execution
- **Task Dependencies**: Proper ETL workflow with error handling
- **Monitoring**: Built-in data quality checks and reporting
- **Scalability**: Designed for production workloads

## 🛠️ Technology Stack

### **Core Technologies**

- **Python 3.8+**: Main programming language
- **Pandas/NumPy**: Data manipulation and analysis
- **SQLAlchemy**: Database operations and ORM
- **PostgreSQL**: Primary data warehouse

### **Infrastructure**

- **Apache Airflow 2.8.1**: Pipeline orchestration
- **Docker & Docker Compose**: Containerized deployment
- **Redis**: Message broker (optional)

### **APIs & Services**

- **Genius API**: Lyrics data source
- **Billboard Charts**: Song ranking data (mock implementation)

## 📊 Data Model

### **Main Tables**

1. **`lyrics_complexity`**: Song-level complexity metrics
2. **`yearly_complexity_stats`**: Yearly aggregated statistics
3. **`decade_complexity_stats`**: Long-term trend analysis

### **Key Metrics**

- **Flesch-Kincaid Score**: Text complexity (lower = more complex)
- **Lexical Diversity**: Vocabulary richness ratio
- **Word Count Analysis**: Total and unique word counts
- **Rank Analysis**: Complexity patterns by chart position

## 🔄 Pipeline Workflow

```
Weekly Schedule (Sunday 2 AM)
    ↓
1. Extract Lyrics (Genius API)
    ↓
2. Transform & Analyze
    ↓
3. Load to PostgreSQL
    ↓
4. Data Quality Check
    ↓
5. Generate Summary Report
    ↓
6. Cleanup Temporary Files
```

## 🚀 Getting Started

### **Quick Setup (5 minutes)**

```bash
# 1. Clone and setup
cd Lyrical-Complexity-Pipeline
python setup.py

# 2. Configure credentials
# Edit config/.env with your Genius API token

# 3. Start with Docker
docker-compose up -d

# 4. Access Airflow UI
# http://localhost:8080
```

### **Manual Execution**

```bash
# Extract lyrics
python scripts/extract_lyrics.py

# Transform data
python scripts/transform_lyrics.py

# Load to database
python scripts/load_to_postgres.py
```

## 📈 What This Demonstrates

### **Data Engineering Skills**

- ✅ **End-to-End Pipeline**: Complete ETL from raw data to insights
- ✅ **API Integration**: Working with external data sources
- ✅ **Data Modeling**: Proper database schema design
- ✅ **Orchestration**: Production-ready workflow management
- ✅ **Data Quality**: Validation, monitoring, and error handling
- ✅ **Performance**: Optimized queries and data loading

### **Modern Data Stack**

- ✅ **Python**: Primary development language
- ✅ **Apache Airflow**: Industry-standard orchestration
- ✅ **PostgreSQL**: Production database
- ✅ **Docker**: Containerized deployment
- ✅ **Configuration Management**: Environment-based setup

### **Business Value**

- ✅ **Unique Analysis**: Goes beyond sentiment to structural complexity
- ✅ **Actionable Insights**: Identifies trends in songwriting evolution
- ✅ **Scalable Design**: Handles growing data volumes
- ✅ **Production Ready**: Includes monitoring and error handling

## 🔍 Sample Insights

The pipeline answers questions like:

- Are hit songs getting simpler or more complex over time?
- Do higher-charting songs have different complexity patterns?
- How does lyrical complexity vary by artist or genre?
- What are the most complex songs in recent years?

## 🎯 Perfect For Interviews

This project demonstrates:

1. **Problem Solving**: Creative approach to music data analysis
2. **Technical Depth**: Full-stack data engineering implementation
3. **Production Thinking**: Error handling, monitoring, scalability
4. **Business Understanding**: Real-world data analysis application
5. **Modern Tools**: Uses exactly what companies are looking for

## 🔮 Future Enhancements

- **Real Billboard API**: Replace mock data with actual chart data
- **Machine Learning**: Predictive models for complexity trends
- **Real-time Streaming**: Live analysis of new releases
- **Multi-language**: Extend to non-English songs
- **Visualization**: Interactive dashboards and charts

## 📚 Learning Outcomes

By building this pipeline, you'll understand:

- How to design end-to-end data pipelines
- Best practices for API integration and rate limiting
- Database schema design and optimization
- Airflow DAG development and scheduling
- Data quality and monitoring strategies
- Production deployment with Docker

---

## 🎉 **This is exactly the kind of project that impresses hiring managers!**

It shows you can:

- **Build from scratch** (0 to 1)
- **Work with real APIs** (data extraction)
- **Perform creative analysis** (complexity metrics)
- **Use modern tools** (Airflow, PostgreSQL, Docker)
- **Think production-ready** (error handling, monitoring)

**The Lyrical Complexity Pipeline is your ticket to demonstrating real data engineering skills!** 🚀
