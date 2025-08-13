# 🎵 Lyrical Complexity Pipeline

A comprehensive data engineering pipeline that analyzes the evolution of lyrical complexity in popular music over time. This project demonstrates end-to-end ETL capabilities by extracting lyrics from Billboard Hot 100 charts, calculating complexity metrics, and orchestrating the entire process with Apache Airflow.

## 🎯 Project Overview

**The Core Question**: Are hit songs getting lyrically simpler or more complex over time?

**The Niche Angle**: This goes beyond simple sentiment analysis and examines the structural properties of lyrics themselves - a unique, data-driven approach to understanding songwriting evolution that's highly relevant to AI music companies.

## 🏗️ Architecture

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

## ✨ Features

- **Lyrics Extraction**: Fetches lyrics for Billboard Hot 100 songs using Genius API
- **Complexity Analysis**: Calculates multiple complexity metrics:
  - Flesch-Kincaid readability score
  - Lexical diversity (type-token ratio)
  - Unique words count
  - Average sentence length
- **Data Transformation**: Cleans, processes, and generates statistical insights
- **Database Storage**: Stores structured data in PostgreSQL with proper indexing
- **Automated Orchestration**: Weekly pipeline execution via Apache Airflow
- **Data Quality Checks**: Comprehensive validation and monitoring
- **Trend Analysis**: Year-over-year complexity evolution tracking

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Genius API access token

### 1. Clone and Setup

```bash
# Navigate to the project directory
cd Lyrical-Complexity-Pipeline

# Copy environment configuration
cp config/env.example config/.env

# Edit the .env file with your credentials
nano config/.env
```

### 2. Install Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install -r requirements/requirements.txt
```

### 3. Configure Environment

Edit `config/.env` with your credentials:

```bash
# Genius API (Required)
GENIUS_ACCESS_TOKEN=your_actual_token_here

# PostgreSQL (Optional - will use Docker if not specified)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=lyrics_complexity
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

### 4. Run with Docker (Recommended)

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### 5. Manual Execution (Alternative)

```bash
# Extract lyrics data
python scripts/extract_lyrics.py

# Transform and analyze data
python scripts/transform_lyrics.py

# Load to database
python scripts/load_to_postgres.py
```

## 📊 Data Pipeline

### Extract Phase

- Fetches Billboard Hot 100 data for the last decade (2015-2024)
- Uses Genius API to retrieve song lyrics
- Implements rate limiting for API respect
- Handles missing lyrics gracefully

### Transform Phase

- Calculates complexity metrics for each song
- Generates yearly and decade statistics
- Performs data cleaning and validation
- Creates complexity categories and rank analysis

### Load Phase

- Creates optimized database schema
- Loads data with proper indexing
- Implements data quality checks
- Generates summary reports

## 🗄️ Database Schema

### Main Tables

#### `lyrics_complexity`

- Primary table storing song-level complexity metrics
- Indexed on year, rank, artist, and complexity categories
- Includes metadata like creation timestamps

#### `yearly_complexity_stats`

- Aggregated statistics by year
- Includes mean, standard deviation, and median values
- Tracks year-over-year complexity trends

#### `decade_complexity_stats`

- Long-term trend analysis by decade
- Provides broader perspective on complexity evolution

## 🔄 Airflow Orchestration

### DAG Schedule

- **Frequency**: Weekly (every Sunday at 2 AM)
- **Dependencies**: Sequential task execution
- **Monitoring**: Built-in data quality checks
- **Reporting**: Automated summary generation

### Task Flow

```
extract_lyrics → transform_lyrics → load_to_postgres →
data_quality_check → generate_summary_report → cleanup_temp_files
```

## 📈 Key Metrics

### Complexity Scores

- **Flesch-Kincaid**: Lower scores = more complex, higher scores = easier to read
- **Lexical Diversity**: Ratio of unique words to total words
- **Word Count Analysis**: Total and unique word counts per song

### Trend Analysis

- Year-over-year complexity evolution
- Rank-based complexity patterns
- Decade-level trend identification

## 🛠️ Configuration Options

### Pipeline Parameters

```bash
START_YEAR=2015          # Analysis start year
END_YEAR=2024           # Analysis end year
BILLBOARD_CHART_SIZE=100 # Number of songs to analyze
```

### API Rate Limiting

```bash
GENIUS_API_DELAY=1      # Seconds between API requests
YEAR_DELAY=5            # Seconds between year processing
```

## 📁 Project Structure

```
Lyrical-Complexity-Pipeline/
├── scripts/                 # Core pipeline scripts
│   ├── extract_lyrics.py   # Lyrics extraction
│   ├── transform_lyrics.py # Data transformation
│   └── load_to_postgres.py # Database loading
├── airflow/                 # Airflow DAGs
│   └── lyrics_complexity_dag.py
├── config/                  # Configuration files
│   ├── env.example         # Environment template
│   └── init.sql            # Database initialization
├── data/                    # Data storage
├── requirements/            # Python dependencies
│   └── requirements.txt
├── docker-compose.yml       # Infrastructure setup
└── README.md               # This file
```

## 🔍 Sample Queries

### Basic Analytics

```sql
-- Overall complexity trend
SELECT year, AVG(flesch_kincaid_score) as avg_complexity
FROM lyrics_complexity
GROUP BY year
ORDER BY year;

-- Most complex songs
SELECT title, artist, flesch_kincaid_score
FROM lyrics_complexity
ORDER BY flesch_kincaid_score ASC
LIMIT 10;

-- Complexity by rank category
SELECT rank_category, AVG(flesch_kincaid_score) as avg_complexity
FROM lyrics_complexity
GROUP BY rank_category
ORDER BY avg_complexity;
```

## 🚨 Troubleshooting

### Common Issues

1. **Genius API Rate Limiting**

   - Increase `GENIUS_API_DELAY` in config
   - Check API token validity

2. **Database Connection Issues**

   - Verify PostgreSQL service is running
   - Check connection credentials in `.env`

3. **Missing Lyrics**
   - Some songs may not have lyrics available
   - Pipeline handles this gracefully

### Debug Mode

```bash
# Enable verbose logging
export LOG_LEVEL=DEBUG

# Run with detailed output
python scripts/extract_lyrics.py --verbose
```

## 🔮 Future Enhancements

- **Real Billboard API Integration**: Replace mock data with actual Billboard API
- **Additional Complexity Metrics**: Rhyme schemes, syllable patterns, vocabulary analysis
- **Machine Learning**: Predictive models for complexity trends
- **Real-time Streaming**: Live complexity analysis of new releases
- **Multi-language Support**: Extend to non-English songs
- **Visualization Dashboard**: Interactive charts and trend analysis

## 📚 Dependencies

### Core Libraries

- `lyricsgenius`: Genius API integration
- `textstat`: Text complexity analysis
- `pandas`: Data manipulation
- `sqlalchemy`: Database operations

### Infrastructure

- `apache-airflow`: Pipeline orchestration
- `postgresql`: Data storage
- `docker`: Containerization

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **Genius.com** for providing lyrics data via API
- **Billboard** for chart data (mock implementation)
- **Apache Airflow** community for orchestration tools
- **PostgreSQL** community for database technology

## 📞 Support

For questions or issues:

- Create a GitHub issue
- Check the troubleshooting section
- Review Airflow logs for detailed error information

---

**Built with ❤️ for data engineers who love music and complexity analysis**
