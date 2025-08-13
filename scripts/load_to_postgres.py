#!/usr/bin/env python3
"""
PostgreSQL Data Loading Script
Loads transformed lyrics complexity data into PostgreSQL database.
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import sys

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PostgresLoader:
    """Loads lyrics complexity data into PostgreSQL database."""
    
    def __init__(self):
        """Initialize the PostgreSQL loader with connection details."""
        self.db_url = os.getenv('POSTGRES_DB_URL')
        if not self.db_url:
            # Fallback to individual environment variables
            host = os.getenv('POSTGRES_HOST', 'localhost')
            port = os.getenv('POSTGRES_PORT', '5432')
            database = os.getenv('POSTGRES_DB', 'lyrics_complexity')
            username = os.getenv('POSTGRES_USER', 'postgres')
            password = os.getenv('POSTGRES_PASSWORD', 'password')
            
            self.db_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        self.engine = None
        self.connection = None
    
    def connect(self):
        """Establish database connection."""
        try:
            self.engine = create_engine(self.db_url)
            self.connection = self.engine.connect()
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    def create_tables(self):
        """Create the necessary database tables."""
        if not self.connection:
            if not self.connect():
                return False
        
        # SQL statements to create tables
        create_tables_sql = """
        -- Main lyrics complexity table
        CREATE TABLE IF NOT EXISTS lyrics_complexity (
            id SERIAL PRIMARY KEY,
            year INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            title VARCHAR(255) NOT NULL,
            artist VARCHAR(255) NOT NULL,
            lyrics_found BOOLEAN DEFAULT FALSE,
            unique_words INTEGER,
            total_words INTEGER,
            avg_sentence_length DECIMAL(5,2),
            flesch_kincaid_score DECIMAL(6,2),
            lexical_diversity DECIMAL(5,3),
            complexity_category VARCHAR(20),
            rank_category VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Yearly statistics table
        CREATE TABLE IF NOT EXISTS yearly_complexity_stats (
            id SERIAL PRIMARY KEY,
            year INTEGER UNIQUE NOT NULL,
            flesch_kincaid_score_mean DECIMAL(6,2),
            flesch_kincaid_score_std DECIMAL(6,2),
            flesch_kincaid_score_median DECIMAL(6,2),
            lexical_diversity_mean DECIMAL(5,3),
            lexical_diversity_std DECIMAL(5,3),
            lexical_diversity_median DECIMAL(5,3),
            unique_words_mean DECIMAL(6,2),
            unique_words_std DECIMAL(6,2),
            unique_words_median DECIMAL(6,2),
            total_words_mean DECIMAL(6,2),
            total_words_std DECIMAL(6,2),
            total_words_median DECIMAL(6,2),
            complexity_trend DECIMAL(6,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Decade statistics table
        CREATE TABLE IF NOT EXISTS decade_complexity_stats (
            id SERIAL PRIMARY KEY,
            decade INTEGER UNIQUE NOT NULL,
            flesch_kincaid_score_mean DECIMAL(6,2),
            lexical_diversity_mean DECIMAL(5,3),
            unique_words_mean DECIMAL(6,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_lyrics_complexity_year ON lyrics_complexity(year);
        CREATE INDEX IF NOT EXISTS idx_lyrics_complexity_rank ON lyrics_complexity(rank);
        CREATE INDEX IF NOT EXISTS idx_lyrics_complexity_artist ON lyrics_complexity(artist);
        CREATE INDEX IF NOT EXISTS idx_lyrics_complexity_complexity_category ON lyrics_complexity(complexity_category);
        """
        
        try:
            # Execute table creation
            for statement in create_tables_sql.split(';'):
                if statement.strip():
                    self.connection.execute(text(statement))
            
            self.connection.commit()
            logger.info("Database tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            self.connection.rollback()
            return False
    
    def load_lyrics_complexity(self, data_file: str):
        """Load the main lyrics complexity data."""
        if not self.connection:
            if not self.connect():
                return False
        
        try:
            # Read the transformed data
            df = pd.read_csv(data_file)
            logger.info(f"Loading {len(df)} records from {data_file}")
            
            # Prepare data for insertion
            df_to_insert = df[['year', 'rank', 'title', 'artist', 'lyrics_found', 
                              'unique_words', 'total_words', 'avg_sentence_length',
                              'flesch_kincaid_score', 'lexical_diversity', 
                              'complexity_category', 'rank_category']].copy()
            
            # Convert year back to integer if it was converted to datetime
            if df_to_insert['year'].dtype == 'object':
                df_to_insert['year'] = pd.to_datetime(df_to_insert['year']).dt.year
            
            # Load data into database
            df_to_insert.to_sql('lyrics_complexity', self.connection, 
                               if_exists='replace', index=False, method='multi')
            
            self.connection.commit()
            logger.info(f"Successfully loaded {len(df_to_insert)} records into lyrics_complexity table")
            return True
            
        except Exception as e:
            logger.error(f"Error loading lyrics complexity data: {str(e)}")
            self.connection.rollback()
            return False
    
    def load_yearly_stats(self, stats_file: str):
        """Load yearly complexity statistics."""
        if not self.connection:
            if not self.connect():
                return False
        
        try:
            # Read the yearly stats
            df = pd.read_csv(stats_file)
            logger.info(f"Loading {len(df)} yearly statistics from {stats_file}")
            
            # Load data into database
            df.to_sql('yearly_complexity_stats', self.connection, 
                      if_exists='replace', index=False, method='multi')
            
            self.connection.commit()
            logger.info(f"Successfully loaded {len(df)} records into yearly_complexity_stats table")
            return True
            
        except Exception as e:
            logger.error(f"Error loading yearly stats: {str(e)}")
            self.connection.rollback()
            return False
    
    def load_decade_stats(self, stats_file: str):
        """Load decade complexity statistics."""
        if not self.connection:
            if not self.connect():
                return False
        
        try:
            # Read the decade stats
            df = pd.read_csv(stats_file)
            logger.info(f"Loading {len(df)} decade statistics from {stats_file}")
            
            # Load data into database
            df.to_sql('decade_complexity_stats', self.connection, 
                      if_exists='replace', index=False, method='multi')
            
            self.connection.commit()
            logger.info(f"Successfully loaded {len(df)} records into decade_complexity_stats table")
            return True
            
        except Exception as e:
            logger.error(f"Error loading decade stats: {str(e)}")
            self.connection.rollback()
            return False
    
    def run_analytics_queries(self):
        """Run some sample analytics queries to verify the data."""
        if not self.connection:
            if not self.connect():
                return False
        
        try:
            # Sample queries to verify data quality
            queries = {
                "Total songs analyzed": "SELECT COUNT(*) FROM lyrics_complexity;",
                "Years covered": "SELECT MIN(year), MAX(year) FROM lyrics_complexity;",
                "Average complexity by rank category": """
                    SELECT rank_category, AVG(flesch_kincaid_score) as avg_complexity 
                    FROM lyrics_complexity 
                    GROUP BY rank_category 
                    ORDER BY avg_complexity;
                """,
                "Complexity trend over years": """
                    SELECT year, AVG(flesch_kincaid_score) as avg_complexity 
                    FROM lyrics_complexity 
                    GROUP BY year 
                    ORDER BY year;
                """,
                "Most complex songs": """
                    SELECT title, artist, flesch_kincaid_score 
                    FROM lyrics_complexity 
                    ORDER BY flesch_kincaid_score ASC 
                    LIMIT 5;
                """
            }
            
            print("\n" + "="*60)
            print("DATABASE ANALYTICS VERIFICATION")
            print("="*60)
            
            for query_name, query in queries.items():
                result = self.connection.execute(text(query))
                data = result.fetchall()
                
                print(f"\n📊 {query_name}:")
                for row in data:
                    print(f"   {row}")
            
            print("="*60)
            return True
            
        except Exception as e:
            logger.error(f"Error running analytics queries: {str(e)}")
            return False
    
    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
        logger.info("Database connection closed")

def main():
    """Main execution function."""
    # Find the transformed data files
    data_dir = Path("../data")
    
    # Look for transformed files
    transformed_file = list(data_dir.glob("*_transformed.csv"))
    yearly_stats_file = list(data_dir.glob("*_yearly_stats.csv"))
    decade_stats_file = list(data_dir.glob("*_decade_stats.csv"))
    
    if not transformed_file:
        logger.error("No transformed data files found")
        logger.info("Please run transform_lyrics.py first")
        return
    
    # Initialize loader
    loader = PostgresLoader()
    
    try:
        # Connect to database
        if not loader.connect():
            return
        
        # Create tables
        if not loader.create_tables():
            return
        
        # Load data
        if not loader.load_lyrics_complexity(str(transformed_file[0])):
            return
        
        if yearly_stats_file and not loader.load_yearly_stats(str(yearly_stats_file[0])):
            return
        
        if decade_stats_file and not loader.load_decade_stats(str(decade_stats_file[0])):
            return
        
        # Run verification queries
        loader.run_analytics_queries()
        
        logger.info("Data loading completed successfully")
        
    except Exception as e:
        logger.error(f"Error during data loading: {str(e)}")
        raise
    finally:
        loader.close()

if __name__ == "__main__":
    main()
