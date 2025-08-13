#!/usr/bin/env python3
"""
Lyrics Transformation Script
Processes extracted lyrics data and calculates additional complexity metrics and trends.
"""

import pandas as pd
import numpy as np
from typing import Dict, List
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LyricsTransformer:
    """Transforms and analyzes lyrics complexity data."""
    
    def __init__(self, input_file: str):
        """Initialize transformer with input data file."""
        self.input_file = input_file
        self.data = None
        
    def load_data(self) -> pd.DataFrame:
        """Load the lyrics complexity data."""
        try:
            self.data = pd.read_csv(self.input_file)
            logger.info(f"Loaded data: {len(self.data)} records")
            return self.data
        except FileNotFoundError:
            logger.error(f"Input file not found: {self.input_file}")
            raise
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def clean_data(self) -> pd.DataFrame:
        """Clean and preprocess the data."""
        if self.data is None:
            self.load_data()
        
        # Remove rows with missing lyrics
        initial_count = len(self.data)
        self.data = self.data.dropna(subset=['lyrics_found'])
        self.data = self.data[self.data['lyrics_found'] == True]
        
        logger.info(f"Removed {initial_count - len(self.data)} records with missing lyrics")
        
        # Convert year to datetime for better analysis
        self.data['year'] = pd.to_datetime(self.data['year'], format='%Y')
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['unique_words', 'total_words', 'avg_sentence_length', 
                          'flesch_kincaid_score', 'lexical_diversity']
        
        for col in numeric_columns:
            if col in self.data.columns:
                self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
        
        # Remove outliers (songs with extremely high complexity scores)
        self.data = self.data[self.data['flesch_kincaid_score'].between(-50, 100)]
        
        logger.info(f"Data cleaned: {len(self.data)} records remaining")
        return self.data
    
    def calculate_additional_metrics(self) -> pd.DataFrame:
        """Calculate additional complexity and trend metrics."""
        if self.data is None:
            self.clean_data()
        
        # Calculate year-over-year complexity trends
        yearly_stats = self.data.groupby(self.data['year'].dt.year).agg({
            'flesch_kincaid_score': ['mean', 'std', 'median'],
            'lexical_diversity': ['mean', 'std', 'median'],
            'unique_words': ['mean', 'std', 'median'],
            'total_words': ['mean', 'std', 'median']
        }).round(3)
        
        # Flatten column names
        yearly_stats.columns = ['_'.join(col).strip() for col in yearly_stats.columns]
        yearly_stats.reset_index(inplace=True)
        yearly_stats.rename(columns={'year': 'year'}, inplace=True)
        
        # Calculate complexity trend (positive = getting simpler, negative = getting more complex)
        yearly_stats['complexity_trend'] = yearly_stats['flesch_kincaid_score_mean'].diff()
        
        # Calculate decade averages
        decade_stats = self.data.groupby(self.data['year'].dt.year // 10 * 10).agg({
            'flesch_kincaid_score': 'mean',
            'lexical_diversity': 'mean',
            'unique_words': 'mean'
        }).round(3)
        
        decade_stats.reset_index(inplace=True)
        decade_stats.rename(columns={'year': 'decade'}, inplace=True)
        
        # Add complexity categories
        self.data['complexity_category'] = pd.cut(
            self.data['flesch_kincaid_score'],
            bins=[-np.inf, 30, 60, 80, np.inf],
            labels=['Very Complex', 'Complex', 'Moderate', 'Simple']
        )
        
        # Calculate rank-based complexity analysis
        self.data['rank_category'] = pd.cut(
            self.data['rank'],
            bins=[0, 10, 25, 50, 100],
            labels=['Top 10', 'Top 25', 'Top 50', 'Top 100']
        )
        
        # Save transformed data
        self.save_transformed_data(yearly_stats, decade_stats)
        
        return self.data
    
    def save_transformed_data(self, yearly_stats: pd.DataFrame, decade_stats: pd.DataFrame):
        """Save the transformed data to files."""
        # Save main transformed data
        output_file = self.input_file.replace('.csv', '_transformed.csv')
        self.data.to_csv(output_file, index=False)
        logger.info(f"Transformed data saved to: {output_file}")
        
        # Save yearly statistics
        yearly_file = self.input_file.replace('.csv', '_yearly_stats.csv')
        yearly_stats.to_csv(yearly_file, index=False)
        logger.info(f"Yearly statistics saved to: {yearly_file}")
        
        # Save decade statistics
        decade_file = self.input_file.replace('.csv', '_decade_stats.csv')
        decade_stats.to_csv(decade_file, index=False)
        logger.info(f"Decade statistics saved to: {decade_file}")
    
    def generate_insights(self) -> Dict:
        """Generate key insights from the transformed data."""
        if self.data is None:
            self.calculate_additional_metrics()
        
        insights = {}
        
        # Overall trends
        insights['total_songs_analyzed'] = len(self.data)
        insights['years_covered'] = f"{self.data['year'].dt.year.min()}-{self.data['year'].dt.year.max()}"
        
        # Complexity trends
        recent_years = self.data[self.data['year'].dt.year >= 2020]
        older_years = self.data[self.data['year'].dt.year < 2020]
        
        if len(recent_years) > 0 and len(older_years) > 0:
            recent_avg = recent_years['flesch_kincaid_score'].mean()
            older_avg = older_years['flesch_kincaid_score'].mean()
            complexity_change = recent_avg - older_avg
            
            insights['complexity_trend'] = {
                'recent_average': round(recent_avg, 2),
                'older_average': round(older_avg, 2),
                'change': round(complexity_change, 2),
                'trend': 'Simpler' if complexity_change > 0 else 'More Complex'
            }
        
        # Top complexity metrics
        insights['top_metrics'] = {
            'most_complex_song': self.data.loc[self.data['flesch_kincaid_score'].idxmin(), 'title'],
            'least_complex_song': self.data.loc[self.data['flesch_kincaid_score'].idxmax(), 'title'],
            'highest_lexical_diversity': self.data.loc[self.data['lexical_diversity'].idxmax(), 'title'],
            'most_unique_words': self.data.loc[self.data['unique_words'].idxmax(), 'title']
        }
        
        # Rank analysis
        rank_analysis = self.data.groupby('rank_category')['flesch_kincaid_score'].mean().round(2)
        insights['rank_complexity'] = rank_analysis.to_dict()
        
        # Yearly complexity distribution
        yearly_complexity = self.data.groupby(self.data['year'].dt.year)['flesch_kincaid_score'].mean().round(2)
        insights['yearly_complexity'] = yearly_complexity.to_dict()
        
        return insights
    
    def print_insights(self, insights: Dict):
        """Print formatted insights to console."""
        print("\n" + "="*60)
        print("LYRICAL COMPLEXITY ANALYSIS INSIGHTS")
        print("="*60)
        
        print(f"\n📊 OVERVIEW:")
        print(f"   Total songs analyzed: {insights['total_songs_analyzed']}")
        print(f"   Years covered: {insights['years_covered']}")
        
        if 'complexity_trend' in insights:
            trend = insights['complexity_trend']
            print(f"\n📈 COMPLEXITY TREND:")
            print(f"   Recent years (2020+): {trend['recent_average']}")
            print(f"   Older years (<2020): {trend['older_average']}")
            print(f"   Change: {trend['change']} ({trend['trend']})")
        
        print(f"\n🏆 TOP METRICS:")
        top = insights['top_metrics']
        print(f"   Most complex song: {top['most_complex_song']}")
        print(f"   Least complex song: {top['least_complex_song']}")
        print(f"   Highest lexical diversity: {top['highest_lexical_diversity']}")
        print(f"   Most unique words: {top['most_unique_words']}")
        
        print(f"\n📊 COMPLEXITY BY RANK:")
        for rank, score in insights['rank_complexity'].items():
            print(f"   {rank}: {score}")
        
        print(f"\n📅 YEARLY COMPLEXITY TREND:")
        for year, score in insights['yearly_complexity'].items():
            print(f"   {year}: {score}")
        
        print("="*60)

def main():
    """Main execution function."""
    # Find the most recent lyrics file
    data_dir = Path("../data")
    lyrics_files = list(data_dir.glob("billboard_hot_100_complexity_*.csv"))
    
    if not lyrics_files:
        logger.error("No lyrics data files found in ../data/")
        logger.info("Please run extract_lyrics.py first")
        return
    
    # Use the most recent file
    input_file = str(max(lyrics_files, key=lambda x: x.stat().st_mtime))
    logger.info(f"Processing file: {input_file}")
    
    # Initialize transformer
    transformer = LyricsTransformer(input_file)
    
    try:
        # Transform the data
        transformed_data = transformer.calculate_additional_metrics()
        
        # Generate insights
        insights = transformer.generate_insights()
        
        # Print insights
        transformer.print_insights(insights)
        
        logger.info("Data transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
