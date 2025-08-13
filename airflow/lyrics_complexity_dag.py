"""
Airflow DAG for Lyrical Complexity Pipeline
Orchestrates the extraction, transformation, and loading of lyrics complexity data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add the scripts directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'lyrics_complexity_pipeline',
    default_args=default_args,
    description='ETL pipeline for analyzing lyrical complexity in Billboard Hot 100 songs',
    schedule_interval='0 2 * * 0',  # Run every Sunday at 2 AM
    catchup=False,
    tags=['lyrics', 'complexity', 'music', 'etl'],
)

# Task 1: Extract lyrics data
def extract_lyrics_data():
    """Extract lyrics data using the extract_lyrics.py script."""
    import subprocess
    import os
    
    # Change to the scripts directory
    scripts_dir = os.path.join(os.path.dirname(__file__), '..', 'scripts')
    os.chdir(scripts_dir)
    
    # Run the extraction script
    result = subprocess.run(['python', 'extract_lyrics.py'], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Extraction failed: {result.stderr}")
    
    print("Lyrics extraction completed successfully")
    return "extraction_success"

extract_task = PythonOperator(
    task_id='extract_lyrics',
    python_callable=extract_lyrics_data,
    dag=dag,
)

# Task 2: Transform lyrics data
def transform_lyrics_data():
    """Transform lyrics data using the transform_lyrics.py script."""
    import subprocess
    import os
    
    # Change to the scripts directory
    scripts_dir = os.path.join(os.path.dirname(__file__), '..', 'scripts')
    os.chdir(scripts_dir)
    
    # Run the transformation script
    result = subprocess.run(['python', 'transform_lyrics.py'], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Transformation failed: {result.stderr}")
    
    print("Lyrics transformation completed successfully")
    return "transformation_success"

transform_task = PythonOperator(
    task_id='transform_lyrics',
    python_callable=transform_lyrics_data,
    dag=dag,
)

# Task 3: Load data to PostgreSQL
def load_to_postgres():
    """Load transformed data to PostgreSQL using the load_to_postgres.py script."""
    import subprocess
    import os
    
    # Change to the scripts directory
    scripts_dir = os.path.join(os.path.dirname(__file__), '..', 'scripts')
    os.chdir(scripts_dir)
    
    # Run the loading script
    result = subprocess.run(['python', 'load_to_postgres.py'], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Loading failed: {result.stderr}")
    
    print("Data loading to PostgreSQL completed successfully")
    return "loading_success"

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

# Task 4: Data quality check
def data_quality_check():
    """Perform data quality checks on the loaded data."""
    import pandas as pd
    import os
    from pathlib import Path
    
    # Find the transformed data file
    data_dir = Path(os.path.join(os.path.dirname(__file__), '..', 'data'))
    transformed_files = list(data_dir.glob("*_transformed.csv"))
    
    if not transformed_files:
        raise Exception("No transformed data files found")
    
    # Read the most recent transformed file
    latest_file = max(transformed_files, key=lambda x: x.stat().st_mtime)
    df = pd.read_csv(latest_file)
    
    # Perform data quality checks
    quality_checks = {
        "total_records": len(df),
        "missing_lyrics": df['lyrics_found'].sum(),
        "avg_complexity": df['flesch_kincaid_score'].mean(),
        "year_range": f"{df['year'].min()}-{df['year'].max()}",
        "rank_range": f"{df['rank'].min()}-{df['rank'].max()}"
    }
    
    # Log quality check results
    print("Data Quality Check Results:")
    for check, value in quality_checks.items():
        print(f"  {check}: {value}")
    
    # Basic validation
    if quality_checks["total_records"] < 100:
        raise Exception(f"Insufficient data: only {quality_checks['total_records']} records found")
    
    if quality_checks["missing_lyrics"] > quality_checks["total_records"] * 0.5:
        raise Exception(f"Too many missing lyrics: {quality_checks['missing_lyrics']} out of {quality_checks['total_records']}")
    
    print("Data quality check passed")
    return "quality_check_passed"

quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)

# Task 5: Generate summary report
def generate_summary_report():
    """Generate a summary report of the pipeline execution."""
    import pandas as pd
    import os
    from pathlib import Path
    from datetime import datetime
    
    # Find the transformed data file
    data_dir = Path(os.path.join(os.path.dirname(__file__), '..', 'data'))
    transformed_files = list(data_dir.glob("*_transformed.csv"))
    
    if not transformed_files:
        raise Exception("No transformed data files found")
    
    # Read the most recent transformed file
    latest_file = max(transformed_files, key=lambda x: x.stat().st_mtime)
    df = pd.read_csv(latest_file)
    
    # Generate summary statistics
    summary = {
        "pipeline_execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_songs_analyzed": len(df),
        "years_covered": f"{df['year'].min()}-{df['year'].max()}",
        "average_complexity_score": round(df['flesch_kincaid_score'].mean(), 2),
        "complexity_trend": "Simpler" if df.groupby('year')['flesch_kincaid_score'].mean().diff().mean() > 0 else "More Complex",
        "top_complexity_song": df.loc[df['flesch_kincaid_score'].idxmin(), 'title'],
        "top_complexity_artist": df.loc[df['flesch_kincaid_score'].idxmin(), 'artist'],
        "least_complexity_song": df.loc[df['flesch_kincaid_score'].idxmax(), 'title'],
        "least_complexity_artist": df.loc[df['flesch_kincaid_score'].idxmax(), 'artist']
    }
    
    # Save summary report
    report_file = data_dir / f"pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    with open(report_file, 'w') as f:
        f.write("LYRICAL COMPLEXITY PIPELINE SUMMARY REPORT\n")
        f.write("=" * 50 + "\n\n")
        
        for key, value in summary.items():
            f.write(f"{key.replace('_', ' ').title()}: {value}\n")
        
        f.write(f"\nReport generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"Summary report generated: {report_file}")
    print("Pipeline Summary:")
    for key, value in summary.items():
        print(f"  {key.replace('_', ' ').title()}: {value}")
    
    return "report_generated"

report_task = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag,
)

# Task 6: Cleanup temporary files
def cleanup_temp_files():
    """Clean up temporary files created during pipeline execution."""
    import os
    from pathlib import Path
    
    # Define directories to clean
    data_dir = Path(os.path.join(os.path.dirname(__file__), '..', 'data'))
    
    # Remove temporary files (keep only the final outputs)
    temp_patterns = [
        "*.tmp",
        "*_temp*",
        "*.log"
    ]
    
    cleaned_files = []
    for pattern in temp_patterns:
        for file_path in data_dir.glob(pattern):
            try:
                file_path.unlink()
                cleaned_files.append(file_path.name)
            except Exception as e:
                print(f"Could not remove {file_path}: {e}")
    
    print(f"Cleaned up {len(cleaned_files)} temporary files")
    return f"cleaned_{len(cleaned_files)}_files"

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task >> quality_check_task >> report_task >> cleanup_task

# Add documentation
dag.doc_md = """
## Lyrical Complexity Pipeline

This DAG orchestrates the complete ETL pipeline for analyzing lyrical complexity in Billboard Hot 100 songs.

### Pipeline Steps:
1. **Extract**: Fetches lyrics data using Genius API for Billboard Hot 100 songs
2. **Transform**: Calculates complexity metrics and generates statistics
3. **Load**: Stores data in PostgreSQL database
4. **Quality Check**: Validates data quality and completeness
5. **Report**: Generates summary report of findings
6. **Cleanup**: Removes temporary files

### Schedule:
- Runs every Sunday at 2 AM
- Processes the last decade of Billboard Hot 100 data
- Calculates Flesch-Kincaid readability scores and lexical diversity metrics

### Outputs:
- CSV files with complexity metrics
- PostgreSQL database with structured data
- Summary reports with key insights
- Trend analysis showing if songs are getting simpler or more complex over time
"""
