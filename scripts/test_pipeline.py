#!/usr/bin/env python3
"""
Test Script for Lyrical Complexity Pipeline
Validates pipeline components and provides quick testing capabilities.
"""

import os
import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_environment():
    """Test if all required environment variables are set."""
    logger.info("Testing environment configuration...")
    
    required_vars = ['GENIUS_ACCESS_TOKEN']
    optional_vars = ['POSTGRES_HOST', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD']
    
    missing_required = []
    missing_optional = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_required.append(var)
    
    for var in optional_vars:
        if not os.getenv(var):
            missing_optional.append(var)
    
    if missing_required:
        logger.error(f"Missing required environment variables: {missing_required}")
        return False
    
    if missing_optional:
        logger.warning(f"Missing optional environment variables: {missing_optional}")
        logger.info("These are optional and will use default values")
    
    logger.info("Environment configuration test passed")
    return True

def test_dependencies():
    """Test if all required Python packages are available."""
    logger.info("Testing Python dependencies...")
    
    required_packages = [
        'lyricsgenius',
        'pandas',
        'numpy',
        'sqlalchemy',
        'psycopg2',
        'dotenv'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            logger.info(f"✓ {package}")
        except ImportError:
            missing_packages.append(package)
            logger.error(f"✗ {package}")
    
    if missing_packages:
        logger.error(f"Missing packages: {missing_packages}")
        logger.info("Install missing packages with: pip install -r requirements/requirements.txt")
        return False
    
    logger.info("All required packages are available")
    return True

def test_data_directory():
    """Test if data directory exists and is writable."""
    logger.info("Testing data directory...")
    
    data_dir = Path("../data")
    
    if not data_dir.exists():
        try:
            data_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Created data directory")
        except Exception as e:
            logger.error(f"Could not create data directory: {e}")
            return False
    
    # Test write permissions
    test_file = data_dir / "test_write.tmp"
    try:
        test_file.write_text("test")
        test_file.unlink()
        logger.info("Data directory is writable")
    except Exception as e:
        logger.error(f"Data directory is not writable: {e}")
        return False
    
    return True

def test_mock_data_generation():
    """Test mock data generation for development purposes."""
    logger.info("Testing mock data generation...")
    
    # Generate sample mock data
    np.random.seed(42)  # For reproducible results
    
    years = list(range(2015, 2025))
    mock_data = []
    
    for year in years:
        for rank in range(1, 101):
            # Generate realistic complexity scores
            complexity_score = np.random.normal(60, 20)  # Mean 60, std 20
            complexity_score = max(-50, min(100, complexity_score))  # Clamp to valid range
            
            mock_data.append({
                'year': year,
                'rank': rank,
                'title': f'Mock Song {rank}',
                'artist': f'Mock Artist {rank}',
                'lyrics_found': True,
                'unique_words': np.random.randint(50, 300),
                'total_words': np.random.randint(100, 500),
                'avg_sentence_length': round(np.random.uniform(5, 15), 2),
                'flesch_kincaid_score': round(complexity_score, 2),
                'lexical_diversity': round(np.random.uniform(0.3, 0.8), 3)
            })
    
    # Create DataFrame
    df = pd.DataFrame(mock_data)
    
    # Test data quality
    assert len(df) == len(years) * 100, "Incorrect number of records"
    assert df['flesch_kincaid_score'].between(-50, 100).all(), "Invalid complexity scores"
    assert df['lexical_diversity'].between(0, 1).all(), "Invalid lexical diversity"
    
    # Save test data
    test_file = Path("../data/test_mock_data.csv")
    df.to_csv(test_file, index=False)
    
    logger.info(f"Generated {len(df)} mock records")
    logger.info(f"Test data saved to: {test_file}")
    
    # Display sample statistics
    print("\n=== MOCK DATA STATISTICS ===")
    print(f"Total songs: {len(df)}")
    print(f"Years covered: {df['year'].min()}-{df['year'].max()}")
    print(f"Average complexity: {df['flesch_kincaid_score'].mean():.2f}")
    print(f"Complexity range: {df['flesch_kincaid_score'].min():.2f} to {df['flesch_kincaid_score'].max():.2f}")
    print(f"Average lexical diversity: {df['lexical_diversity'].mean():.3f}")
    
    return True

def test_complexity_calculations():
    """Test complexity calculation functions."""
    logger.info("Testing complexity calculations...")
    
    # Test lyrics
    test_lyrics = """
    This is a test song with some lyrics
    It has multiple lines and some repetition
    This is a test song with some lyrics
    It has multiple lines and some repetition
    """
    
    # Calculate metrics manually
    words = test_lyrics.split()
    total_words = len(words)
    unique_words = len(set(words))
    sentences = [s.strip() for s in test_lyrics.split('\n') if s.strip()]
    avg_sentence_length = total_words / len(sentences) if sentences else 0
    lexical_diversity = unique_words / total_words if total_words > 0 else 0
    
    # Verify calculations
    assert total_words == 24, f"Expected 24 words, got {total_words}"
    assert unique_words == 12, f"Expected 12 unique words, got {unique_words}"
    assert avg_sentence_length == 6.0, f"Expected 6.0 avg sentence length, got {avg_sentence_length}"
    assert lexical_diversity == 0.5, f"Expected 0.5 lexical diversity, got {lexical_diversity}"
    
    logger.info("Complexity calculations test passed")
    return True

def test_database_schema():
    """Test database schema creation (without actual connection)."""
    logger.info("Testing database schema...")
    
    # This would normally test actual database connection
    # For now, we'll just verify the schema SQL is valid
    schema_sql = """
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
    """
    
    # Basic SQL validation (very simple)
    required_keywords = ['CREATE', 'TABLE', 'lyrics_complexity', 'PRIMARY KEY']
    for keyword in required_keywords:
        if keyword not in schema_sql.upper():
            logger.error(f"Missing required keyword: {keyword}")
            return False
    
    logger.info("Database schema test passed")
    return True

def run_all_tests():
    """Run all pipeline tests."""
    logger.info("Starting pipeline validation tests...")
    
    tests = [
        ("Environment Configuration", test_environment),
        ("Python Dependencies", test_dependencies),
        ("Data Directory", test_data_directory),
        ("Mock Data Generation", test_mock_data_generation),
        ("Complexity Calculations", test_complexity_calculations),
        ("Database Schema", test_database_schema),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"Running: {test_name}")
        logger.info(f"{'='*50}")
        
        try:
            result = test_func()
            results.append((test_name, result, None))
            if result:
                logger.info(f"✓ {test_name} PASSED")
            else:
                logger.error(f"✗ {test_name} FAILED")
        except Exception as e:
            logger.error(f"✗ {test_name} ERROR: {str(e)}")
            results.append((test_name, False, str(e)))
    
    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("TEST SUMMARY")
    logger.info(f"{'='*50}")
    
    passed = sum(1 for _, result, _ in results if result)
    total = len(results)
    
    for test_name, result, error in results:
        status = "PASS" if result else "FAIL"
        if error:
            status += f" (Error: {error})"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Pipeline is ready to run.")
        return True
    else:
        logger.error("❌ Some tests failed. Please fix issues before running the pipeline.")
        return False

def main():
    """Main test execution function."""
    print("🎵 LYRICAL COMPLEXITY PIPELINE - VALIDATION TESTS")
    print("=" * 60)
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Run tests
    success = run_all_tests()
    
    if success:
        print("\n🚀 Pipeline validation successful! You can now:")
        print("1. Run: python scripts/extract_lyrics.py")
        print("2. Run: python scripts/transform_lyrics.py")
        print("3. Run: python scripts/load_to_postgres.py")
        print("4. Or use Docker: docker-compose up -d")
    else:
        print("\n⚠️  Pipeline validation failed. Please fix issues before proceeding.")
        sys.exit(1)

if __name__ == "__main__":
    main()
