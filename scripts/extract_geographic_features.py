"""
Module 1: Geographic Features Extraction using OSMnx
Extracts nearby physical features such as roads, industrial zones, dump sites, 
and agricultural fields using OpenStreetMap via OSMnx library.
Tags each feature with latitude, longitude, timestamp, and source metadata.
"""

import pandas as pd
import geopandas as gpd
import osmnx as ox
import numpy as np
import json
import os
from datetime import datetime, timezone
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
import warnings
warnings.filterwarnings('ignore')

# Configure OSMnx
ox.config(use_cache=True, log_console=True)

class GeographicFeaturesExtractor:
    """Extract geographic features around monitoring locations using OSMnx"""
    
    def __init__(self, locations_file="data/global_locations_cleaned.csv"):
        self.locations_file = locations_file
        self.output_dir = "data"
        
        # Define feature categories and their OSM tags
        self.feature_definitions = {
            'roads': {
                'tags': {'highway': True},
                'description': 'All types of roads and highways'
            },
            'industrial_zones': {
                'tags': {
                    'landuse': ['industrial', 'commercial', 'retail'],
                    'amenity': ['fuel', 'parking'],
                    'building': ['industrial', 'commercial', 'retail']
                },
                'description': 'Industrial and commercial areas'
            },
            'dump_sites': {
                'tags': {
                    'landuse': ['landfill'],
                    'amenity': ['waste_disposal', 'waste_transfer_station', 'recycling'],
                    'man_made': ['wastewater_plant'],
                    'industrial': ['scrap_yard']
                },
                'description': 'Waste disposal and treatment facilities'
            },
            'agricultural_fields': {
                'tags': {
                    'landuse': ['farmland', 'farmyard', 'orchard', 'vineyard', 'meadow', 'grass'],
                    'natural': ['grassland']
                },
                'description': 'Agricultural and farming areas'
            },
            'water_bodies': {
                'tags': {
                    'natural': ['water'],
                    'waterway': True,
                    'landuse': ['reservoir', 'basin']
                },
                'description': 'Rivers, lakes, and water bodies'
            },
            'green_spaces': {
                'tags': {
                    'landuse': ['forest', 'recreation_ground'],
                    'natural': ['wood', 'tree', 'park'],
                    'leisure': ['park', 'garden', 'nature_reserve']
                },
                'description': 'Parks, forests, and green areas'
            }
        }
    
    def load_locations(self):
        """Load monitoring locations from CSV file"""
        if not os.path.exists(self.locations_file):
            raise FileNotFoundError(f"Locations file not found: {self.locations_file}")
        
        df = pd.read_csv(self.locations_file)
        
        # Parse coordinates if they're in string format
        if 'coordinates' in df.columns:
            df['latitude'] = df['coordinates'].apply(
                lambda x: eval(x).get('latitude') if pd.notna(x) and x != '' else None
            )
            df['longitude'] = df['coordinates'].apply(
                lambda x: eval(x).get('longitude') if pd.notna(x) and x != '' else None
            )
        
        # Filter locations with valid coordinates
        valid_locations = df.dropna(subset=['latitude', 'longitude'])
        print(f"Loaded {len(valid_locations)} locations with valid coordinates")
        
        return valid_locations
    
    def extract_features_around_point(self, lat, lon, radius_km=5):
        """Extract geographic features around a specific point"""
        features_data = {}
        
        try:
            # Create a point and buffer around it
            point = Point(lon, lat)
            
            # Convert radius from km to degrees (approximate)
            radius_deg = radius_km / 111.32  # roughly 111.32 km per degree
            
            # Define bounding box
            north = lat + radius_deg
            south = lat - radius_deg
            east = lon + radius_deg
            west = lon - radius_deg
            
            print(f"Extracting features around ({lat:.4f}, {lon:.4f}) within {radius_km}km...")
            
            # Extract each feature type
            for feature_type, config in self.feature_definitions.items():
                try:
                    print(f"  - Extracting {feature_type}...")
                    
                    # Get features from OSM
                    gdf = ox.features_from_bbox(
                        north=north, south=south, east=east, west=west,
                        tags=config['tags']
                    )
                    
                    if not gdf.empty:
                        # Calculate distance to monitoring point for each feature
                        gdf['distance_km'] = gdf.geometry.apply(
                            lambda geom: point.distance(geom.centroid) * 111.32
                        )
                        
                        # Filter features within radius
                        nearby_features = gdf[gdf['distance_km'] <= radius_km]
                        
                        if not nearby_features.empty:
                            features_data[feature_type] = {
                                'count': len(nearby_features),
                                'closest_distance_km': nearby_features['distance_km'].min(),
                                'average_distance_km': nearby_features['distance_km'].mean(),
                                'features': []
                            }
                            
                            # Store individual feature details (limit to avoid huge files)
                            for idx, feature in nearby_features.head(10).iterrows():
                                feature_info = {
                                    'osm_id': str(feature.get('osmid', '')),
                                    'name': feature.get('name', ''),
                                    'feature_type': feature.get('landuse', feature.get('highway', feature.get('amenity', ''))),
                                    'distance_km': round(feature['distance_km'], 3),
                                    'geometry_type': str(feature.geometry.geom_type)
                                }
                                features_data[feature_type]['features'].append(feature_info)
                        else:
                            features_data[feature_type] = {
                                'count': 0,
                                'closest_distance_km': None,
                                'average_distance_km': None,
                                'features': []
                            }
                    else:
                        features_data[feature_type] = {
                            'count': 0,
                            'closest_distance_km': None,
                            'average_distance_km': None,
                            'features': []
                        }
                        
                except Exception as e:
                    print(f"    Error extracting {feature_type}: {e}")
                    features_data[feature_type] = {
                        'count': 0,
                        'closest_distance_km': None,
                        'average_distance_km': None,
                        'features': [],
                        'error': str(e)
                    }
        
        except Exception as e:
            print(f"Error extracting features around ({lat}, {lon}): {e}")
            return None
        
        return features_data
    
    def create_feature_summary(self, location_info, features_data):
        """Create a flat summary of features for CSV export"""
        summary = {
            'location_id': location_info['id'],
            'location_name': location_info.get('name', ''),
            'latitude': location_info['latitude'],
            'longitude': location_info['longitude'],
            'country': location_info.get('country', ''),
            'city': location_info.get('city', ''),
            'extraction_radius_km': 5,
            'source_api': 'OpenStreetMap_OSMnx',
            'extracted_timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # Add feature counts and distances
        for feature_type, data in features_data.items():
            summary[f'{feature_type}_count'] = data['count']
            summary[f'{feature_type}_closest_km'] = data['closest_distance_km']
            summary[f'{feature_type}_avg_distance_km'] = data['average_distance_km']
        
        return summary
    
    def extract_all_features(self, sample_size=None, radius_km=5):
        """Extract geographic features for all monitoring locations"""
        
        # Load locations
        locations_df = self.load_locations()
        
        if sample_size:
            locations_df = locations_df.head(sample_size)
            print(f"Processing sample of {sample_size} locations")
        
        all_summaries = []
        all_detailed_features = []
        
        print(f"Starting geographic features extraction for {len(locations_df)} locations...")
        print(f"Search radius: {radius_km} km")
        
        for idx, location in locations_df.iterrows():
            location_info = location.to_dict()
            lat, lon = location['latitude'], location['longitude']
            
            print(f"\nProcessing location {idx+1}/{len(locations_df)}: {location.get('name', 'Unknown')}")
            
            # Extract features around this location
            features_data = self.extract_features_around_point(lat, lon, radius_km)
            
            if features_data:
                # Create summary for CSV
                summary = self.create_feature_summary(location_info, features_data)
                all_summaries.append(summary)
                
                # Store detailed features for JSON
                detailed_record = {
                    'location': location_info,
                    'features': features_data,
                    'metadata': {
                        'extraction_radius_km': radius_km,
                        'source_api': 'OpenStreetMap_OSMnx',
                        'extracted_timestamp': datetime.now(timezone.utc).isoformat()
                    }
                }
                all_detailed_features.append(detailed_record)
            
            # Save progress every 10 locations
            if (idx + 1) % 10 == 0:
                temp_df = pd.DataFrame(all_summaries)
                temp_df.to_csv(f"{self.output_dir}/geographic_features_temp.csv", index=False)
                print(f"Temporary save: {len(all_summaries)} locations processed")
        
        # Save final results
        if all_summaries:
            # Save summary CSV
            summary_df = pd.DataFrame(all_summaries)
            summary_file = f"{self.output_dir}/geographic_features_summary.csv"
            summary_df.to_csv(summary_file, index=False)
            
            # Save detailed JSON
            detailed_file = f"{self.output_dir}/geographic_features_detailed.json"
            with open(detailed_file, 'w') as f:
                json.dump(all_detailed_features, f, indent=2)
            
            print(f"\nGeographic features extraction completed!")
            print(f"Processed {len(summary_df)} locations")
            print(f"Results saved to:")
            print(f"  - Summary: {summary_file}")
            print(f"  - Detailed: {detailed_file}")
            
            # Show feature statistics
            print(f"\nFeature extraction statistics:")
            for feature_type in self.feature_definitions.keys():
                count_col = f'{feature_type}_count'
                if count_col in summary_df.columns:
                    total_features = summary_df[count_col].sum()
                    avg_features = summary_df[count_col].mean()
                    print(f"  - {feature_type}: {total_features} total, {avg_features:.1f} avg per location")
            
            # Clean up temp file
            temp_file = f"{self.output_dir}/geographic_features_temp.csv"
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        else:
            print("No geographic features extracted.")

def main():
    """Main function to run geographic features extraction"""
    extractor = GeographicFeaturesExtractor()
    
    # Extract features for all locations (or specify sample_size for testing)
    # For testing, you can use: extractor.extract_all_features(sample_size=5)
    extractor.extract_all_features(sample_size=20, radius_km=3)  # Start with smaller sample and radius

if __name__ == "__main__":
    main()