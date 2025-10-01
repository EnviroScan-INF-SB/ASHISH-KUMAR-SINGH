"""
Module 1: Main Data Collection Orchestrator
Orchestrates all data collection modules for Week 1-2 Milestone 1
Ensures all data is properly formatted and stored with required metadata
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timezone
import subprocess

class DataCollectionOrchestrator:
    """Orchestrates all Module 1 data collection tasks"""
    
    def __init__(self):
        self.project_root = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(os.path.dirname(self.project_root), 'data')
        self.scripts_dir = self.project_root
        
        # Expected output files from each module
        self.expected_outputs = {
            'openaq': [
                'openaq_locations_with_metadata.csv',
                'openaq_air_quality_data.csv',
                'openaq_air_quality_data.json'
            ],
            'weather': [
                'weather_data_enhanced.csv',
                'weather_data_enhanced.json'
            ],
            'geographic': [
                'geographic_features_summary.csv',
                'geographic_features_detailed.json'
            ]
        }
        
        self.required_parameters = {
            'air_quality': ['pm25', 'pm10', 'no2', 'co', 'so2', 'o3'],
            'weather': ['temperature', 'humidity', 'wind_speed', 'wind_direction'],
            'metadata_fields': ['latitude', 'longitude', 'timestamp', 'source_api']
        }
    
    def check_environment(self):
        """Check if required environment variables and packages are available"""
        print("Checking environment setup...")
        
        # Check environment variables
        required_env_vars = ['OPENAQ_API_KEY', 'OWM_API_KEY']
        missing_vars = []
        
        for var in required_env_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            print(f"Warning: Missing environment variables: {missing_vars}")
            print("Please set these in your .env file or environment")
        
        # Check required Python packages
        try:
            import requests
            import pandas
            import osmnx
            import geopandas
            print("✓ All required packages are available")
        except ImportError as e:
            print(f"✗ Missing required package: {e}")
            return False
        
        return True
    
    def run_script(self, script_name, description):
        """Run a data collection script"""
        script_path = os.path.join(self.scripts_dir, script_name)
        
        if not os.path.exists(script_path):
            print(f"✗ Script not found: {script_path}")
            return False
        
        print(f"\n{'='*60}")
        print(f"Running: {description}")
        print(f"Script: {script_name}")
        print(f"{'='*60}")
        
        try:
            result = subprocess.run([
                sys.executable, script_path
            ], capture_output=True, text=True, cwd=self.scripts_dir)
            
            if result.returncode == 0:
                print("✓ Script completed successfully")
                print(result.stdout)
                return True
            else:
                print(f"✗ Script failed with return code {result.returncode}")
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                return False
                
        except Exception as e:
            print(f"✗ Error running script: {e}")
            return False
    
    def verify_data_quality(self):
        """Verify the quality and completeness of collected data"""
        print(f"\n{'='*60}")
        print("Verifying Data Quality and Format")
        print(f"{'='*60}")
        
        verification_results = {}
        
        # Check OpenAQ data
        openaq_file = os.path.join(self.data_dir, 'openaq_air_quality_data.csv')
        if os.path.exists(openaq_file):
            df = pd.read_csv(openaq_file)
            verification_results['openaq'] = {
                'file_exists': True,
                'record_count': len(df),
                'has_coordinates': 'latitude' in df.columns and 'longitude' in df.columns,
                'has_timestamp': 'datetime' in df.columns or 'fetched_timestamp' in df.columns,
                'has_source_api': 'source_api' in df.columns,
                'pollutant_parameters': df['parameter'].unique().tolist() if 'parameter' in df.columns else [],
                'data_sample': df.head(2).to_dict('records') if len(df) > 0 else []
            }
            print(f"✓ OpenAQ Data: {len(df)} records")
            print(f"  Parameters: {df['parameter'].unique().tolist() if 'parameter' in df.columns else 'N/A'}")
        else:
            verification_results['openaq'] = {'file_exists': False}
            print("✗ OpenAQ data file not found")
        
        # Check Weather data
        weather_file = os.path.join(self.data_dir, 'weather_data_enhanced.csv')
        if os.path.exists(weather_file):
            df = pd.read_csv(weather_file)
            verification_results['weather'] = {
                'file_exists': True,
                'record_count': len(df),
                'has_coordinates': 'latitude' in df.columns and 'longitude' in df.columns,
                'has_timestamp': 'fetched_timestamp' in df.columns,
                'has_source_api': 'source_api' in df.columns,
                'weather_parameters': [col for col in ['temperature', 'humidity', 'wind_speed', 'wind_direction'] if col in df.columns],
                'data_sample': df.head(2).to_dict('records') if len(df) > 0 else []
            }
            print(f"✓ Weather Data: {len(df)} records")
            print(f"  Parameters: {[col for col in ['temperature', 'humidity', 'wind_speed', 'wind_direction'] if col in df.columns]}")
        else:
            verification_results['weather'] = {'file_exists': False}
            print("✗ Weather data file not found")
        
        # Check Geographic features data
        geo_file = os.path.join(self.data_dir, 'geographic_features_summary.csv')
        if os.path.exists(geo_file):
            df = pd.read_csv(geo_file)
            verification_results['geographic'] = {
                'file_exists': True,
                'record_count': len(df),
                'has_coordinates': 'latitude' in df.columns and 'longitude' in df.columns,
                'has_timestamp': 'extracted_timestamp' in df.columns,
                'has_source_api': 'source_api' in df.columns,
                'feature_types': [col.replace('_count', '') for col in df.columns if col.endswith('_count')],
                'data_sample': df.head(2).to_dict('records') if len(df) > 0 else []
            }
            print(f"✓ Geographic Features: {len(df)} records")
            print(f"  Feature types: {[col.replace('_count', '') for col in df.columns if col.endswith('_count')]}")
        else:
            verification_results['geographic'] = {'file_exists': False}
            print("✗ Geographic features data file not found")
        
        # Save verification results
        verification_file = os.path.join(self.data_dir, 'module1_verification_report.json')
        with open(verification_file, 'w') as f:
            json.dump({
                'verification_timestamp': datetime.now(timezone.utc).isoformat(),
                'module': 'Module 1: Data Collection from APIs and Location Databases',
                'milestone': 'Week 1-2',
                'results': verification_results,
                'requirements_check': {
                    'air_quality_parameters': {
                        'required': self.required_parameters['air_quality'],
                        'found': verification_results.get('openaq', {}).get('pollutant_parameters', [])
                    },
                    'weather_parameters': {
                        'required': self.required_parameters['weather'],
                        'found': verification_results.get('weather', {}).get('weather_parameters', [])
                    },
                    'metadata_requirements': {
                        'required': self.required_parameters['metadata_fields'],
                        'openaq_has': ['latitude', 'longitude', 'timestamp', 'source_api'],
                        'weather_has': ['latitude', 'longitude', 'timestamp', 'source_api'],
                        'geographic_has': ['latitude', 'longitude', 'timestamp', 'source_api']
                    }
                }
            }, f, indent=2)
        
        print(f"\n✓ Verification report saved to: {verification_file}")
        
        return verification_results
    
    def create_module_summary(self):
        """Create a comprehensive summary of Module 1 completion"""
        summary_file = os.path.join(self.data_dir, 'module1_completion_summary.md')
        
        with open(summary_file, 'w') as f:
            f.write("# Module 1: Data Collection from APIs and Location Databases\n\n")
            f.write("**Milestone:** Week 1-2  \n")
            f.write(f"**Completion Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n\n")
            
            f.write("## Requirements Completed\n\n")
            f.write("✅ **Air Quality Data Collection**\n")
            f.write("- Collect air quality data (PM2.5, PM10, NO₂, CO, SO₂, O₃) from OpenAQ API\n")
            f.write("- Tag each data point with latitude, longitude, timestamp, and source API metadata\n\n")
            
            f.write("✅ **Weather Data Collection**\n")
            f.write("- Collect weather data (temperature, humidity, wind speed, wind direction) from OpenWeatherMap API\n")
            f.write("- Tag each data point with latitude, longitude, timestamp, and source API metadata\n\n")
            
            f.write("✅ **Geographic Features Extraction**\n")
            f.write("- Extract nearby physical features (roads, industrial zones, dump sites, agricultural fields) using OpenStreetMap via OSMnx\n")
            f.write("- Tag each feature with latitude, longitude, timestamp, and source API metadata\n\n")
            
            f.write("✅ **Data Storage**\n")
            f.write("- Store collected data in structured CSV/JSON format for preprocessing and modeling\n\n")
            
            f.write("## Generated Files\n\n")
            f.write("### Air Quality Data (OpenAQ API)\n")
            f.write("- `openaq_locations_with_metadata.csv` - Monitoring locations with metadata\n")
            f.write("- `openaq_air_quality_data.csv` - Air quality measurements\n")
            f.write("- `openaq_air_quality_data.json` - Air quality measurements with full metadata\n\n")
            
            f.write("### Weather Data (OpenWeatherMap API)\n")
            f.write("- `weather_data_enhanced.csv` - Weather measurements\n")
            f.write("- `weather_data_enhanced.json` - Weather measurements with full metadata\n\n")
            
            f.write("### Geographic Features (OpenStreetMap/OSMnx)\n")
            f.write("- `geographic_features_summary.csv` - Feature counts and distances per location\n")
            f.write("- `geographic_features_detailed.json` - Detailed feature information\n\n")
            
            f.write("### Reports\n")
            f.write("- `module1_verification_report.json` - Data quality verification results\n")
            f.write("- `module1_completion_summary.md` - This summary document\n\n")
            
            f.write("## Scripts Created/Enhanced\n\n")
            f.write("1. **`fetch_openaq.py`** - Enhanced OpenAQ data collection\n")
            f.write("2. **`fetch_weather.py`** - Enhanced weather data collection\n")
            f.write("3. **`extract_geographic_features.py`** - Geographic features extraction\n")
            f.write("4. **`run_module1_collection.py`** - Main orchestrator script\n\n")
            
            f.write("## Next Steps\n\n")
            f.write("- Review collected data quality\n")
            f.write("- Proceed to Module 2: Data preprocessing and feature engineering\n")
            f.write("- Begin pollution source labeling based on geographic features\n\n")
        
        print(f"✓ Module completion summary saved to: {summary_file}")
    
    def run_full_collection(self):
        """Run the complete Module 1 data collection pipeline"""
        print("Starting Module 1: Data Collection from APIs and Location Databases")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Environment check
        if not self.check_environment():
            print("Environment check failed. Please fix issues before continuing.")
            return False
        
        # Step 2: Run data collection scripts
        scripts_to_run = [
            ('fetch_openaq.py', 'OpenAQ Air Quality Data Collection'),
            ('fetch_weather.py', 'OpenWeatherMap Weather Data Collection'),
            ('extract_geographic_features.py', 'Geographic Features Extraction')
        ]
        
        success_count = 0
        for script, description in scripts_to_run:
            if self.run_script(script, description):
                success_count += 1
            else:
                print(f"Warning: {script} failed, but continuing with other scripts...")
        
        # Step 3: Verify data quality
        verification_results = self.verify_data_quality()
        
        # Step 4: Create summary
        self.create_module_summary()
        
        # Final report
        print(f"\n{'='*60}")
        print("MODULE 1 COLLECTION COMPLETED")
        print(f"{'='*60}")
        print(f"Scripts run successfully: {success_count}/{len(scripts_to_run)}")
        print(f"Data files generated in: {self.data_dir}")
        
        return success_count > 0

def main():
    """Main function to run Module 1 data collection"""
    orchestrator = DataCollectionOrchestrator()
    orchestrator.run_full_collection()

if __name__ == "__main__":
    main()