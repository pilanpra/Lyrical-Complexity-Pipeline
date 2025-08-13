#!/usr/bin/env python3
"""
Lyrics Extraction Script for Billboard Hot 100 Analysis
Fetches lyrics for songs from Billboard Hot 100 charts and calculates complexity metrics.
"""

import os
import time
import logging
from typing import List, Dict, Optional
import pandas as pd
import lyricsgenius
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LyricsExtractor:
    """Extracts lyrics and calculates complexity metrics for Billboard Hot 100 songs."""
    
    def __init__(self, genius_token: str):
        """Initialize the lyrics extractor with Genius API token."""
        self.genius = lyricsgenius.Genius(genius_token)
        self.genius.verbose = False
        self.genius.remove_section_headers = True
        
    def get_billboard_hot_100(self, year: int) -> List[Dict]:
        """
        Get Billboard Hot 100 songs for a specific year.
        Note: This is a mock implementation since Billboard API requires subscription.
        In production, you would use the official Billboard API.
        """
        # Mock data for demonstration - replace with actual Billboard API call
        mock_hot_100 = [
            {"rank": 1, "title": "Blinding Lights", "artist": "The Weeknd", "year": year},
            {"rank": 2, "title": "Dance Monkey", "artist": "Tones and I", "year": year},
            {"rank": 3, "title": "The Box", "artist": "Roddy Ricch", "year": year},
            {"rank": 4, "title": "Don't Start Now", "artist": "Dua Lipa", "year": year},
            {"rank": 5, "title": "Someone You Loved", "artist": "Lewis Capaldi", "year": year},
            {"rank": 6, "title": "Circles", "artist": "Post Malone", "year": year},
            {"rank": 7, "title": "Sunflower", "artist": "Post Malone & Swae Lee", "year": year},
            {"rank": 8, "title": "Shallow", "artist": "Lady Gaga & Bradley Cooper", "year": year},
            {"rank": 9, "title": "Old Town Road", "artist": "Lil Nas X", "year": year},
            {"rank": 10, "title": "Bad Guy", "artist": "Billie Eilish", "year": year},
        ]
        
        # Add more mock songs to reach 100
        for i in range(11, 101):
            mock_hot_100.append({
                "rank": i,
                "title": f"Mock Song {i}",
                "artist": f"Mock Artist {i}",
                "year": year
            })
            
        return mock_hot_100
    
    def search_song_lyrics(self, title: str, artist: str) -> Optional[str]:
        """Search for song lyrics using Genius API."""
        try:
            # Search for the song
            search_query = f"{title} {artist}"
            search_results = self.genius.search_song(search_query)
            
            if search_results:
                return search_results.lyrics
            else:
                logger.warning(f"No lyrics found for: {search_query}")
                return None
                
        except Exception as e:
            logger.error(f"Error searching for lyrics: {title} by {artist}: {str(e)}")
            return None
    
    def calculate_complexity_metrics(self, lyrics: str) -> Dict:
        """Calculate various complexity metrics for the given lyrics."""
        if not lyrics:
            return {
                "unique_words": 0,
                "total_words": 0,
                "avg_sentence_length": 0,
                "flesch_kincaid_score": 0,
                "lexical_diversity": 0
            }
        
        # Basic text statistics
        words = lyrics.split()
        total_words = len(words)
        unique_words = len(set(words))
        
        # Calculate average sentence length
        sentences = [s.strip() for s in lyrics.split('\n') if s.strip()]
        avg_sentence_length = total_words / len(sentences) if sentences else 0
        
        # Lexical diversity (type-token ratio)
        lexical_diversity = unique_words / total_words if total_words > 0 else 0
        
        # Flesch-Kincaid readability score (simplified calculation)
        # Lower score = more complex, higher score = easier to read
        syllables = sum(1 for char in lyrics.lower() if char in 'aeiou')
        flesch_kincaid = 206.835 - (1.015 * avg_sentence_length) - (84.6 * (syllables / total_words)) if total_words > 0 else 0
        
        return {
            "unique_words": unique_words,
            "total_words": total_words,
            "avg_sentence_length": round(avg_sentence_length, 2),
            "flesch_kincaid_score": round(flesch_kincaid, 2),
            "lexical_diversity": round(lexical_diversity, 3)
        }
    
    def process_year(self, year: int) -> pd.DataFrame:
        """Process all songs for a specific year and return complexity metrics."""
        logger.info(f"Processing Billboard Hot 100 for year {year}")
        
        # Get Billboard Hot 100 for the year
        hot_100 = self.get_billboard_hot_100(year)
        
        results = []
        
        for song in hot_100:
            logger.info(f"Processing: {song['title']} by {song['artist']}")
            
            # Search for lyrics
            lyrics = self.search_song_lyrics(song['title'], song['artist'])
            
            # Calculate complexity metrics
            metrics = self.calculate_complexity_metrics(lyrics)
            
            # Combine song info with metrics
            song_data = {
                "year": year,
                "rank": song['rank'],
                "title": song['title'],
                "artist": song['artist'],
                "lyrics_found": lyrics is not None,
                **metrics
            }
            
            results.append(song_data)
            
            # Rate limiting to be respectful to the API
            time.sleep(1)
        
        return pd.DataFrame(results)
    
    def process_multiple_years(self, start_year: int, end_year: int) -> pd.DataFrame:
        """Process multiple years and combine results."""
        all_results = []
        
        for year in range(start_year, end_year + 1):
            year_results = self.process_year(year)
            all_results.append(year_results)
            
            # Add delay between years
            time.sleep(5)
        
        return pd.concat(all_results, ignore_index=True)

def main():
    """Main execution function."""
    # Get Genius API token from environment
    genius_token = os.getenv('GENIUS_ACCESS_TOKEN')
    
    if not genius_token:
        logger.error("GENIUS_ACCESS_TOKEN not found in environment variables")
        logger.info("Please set GENIUS_ACCESS_TOKEN in your .env file")
        return
    
    # Initialize extractor
    extractor = LyricsExtractor(genius_token)
    
    # Process last decade (2015-2024)
    start_year = 2015
    end_year = 2024
    
    logger.info(f"Starting lyrics extraction for years {start_year}-{end_year}")
    
    try:
        # Process all years
        results_df = extractor.process_multiple_years(start_year, end_year)
        
        # Save results
        output_file = f"../data/billboard_hot_100_complexity_{start_year}_{end_year}.csv"
        results_df.to_csv(output_file, index=False)
        
        logger.info(f"Successfully processed {len(results_df)} songs")
        logger.info(f"Results saved to: {output_file}")
        
        # Display summary statistics
        print("\n=== LYRICAL COMPLEXITY ANALYSIS SUMMARY ===")
        print(f"Total songs analyzed: {len(results_df)}")
        print(f"Years covered: {start_year}-{end_year}")
        print(f"Average Flesch-Kincaid score: {results_df['flesch_kincaid_score'].mean():.2f}")
        print(f"Average lexical diversity: {results_df['lexical_diversity'].mean():.3f}")
        print(f"Average unique words per song: {results_df['unique_words'].mean():.1f}")
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        raise

if __name__ == "__main__":
    main()
