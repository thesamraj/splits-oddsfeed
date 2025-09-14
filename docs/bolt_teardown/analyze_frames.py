#!/usr/bin/env python3
"""
Analyze captured BoltOdds frames
"""
import json
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any

class FrameAnalyzer:
    def __init__(self):
        self.frames = []
        self.msg_types = defaultdict(int)
        self.field_schema = defaultdict(lambda: {
            'count': 0,
            'types': set(),
            'samples': [],
            'nullable': False
        })
        self.data_frames = []
        
    def load_frames(self, raw_dir: Path = Path('data/bolt/raw')):
        """Load all captured frames"""
        for file_path in sorted(raw_dir.glob('frames_*.jsonl')):
            with open(file_path) as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        self.frames.append(record)
                        
                        # Categorize frame
                        data = record.get('data', {})
                        if 'action' in data:
                            self.msg_types[data['action']] += 1
                            if data['action'] not in ['ping', 'socket_connected', 'subscribe']:
                                self.data_frames.append(data)
                        elif 'type' in data:
                            self.msg_types[data['type']] += 1
                            if data['type'] not in ['subscription_sent']:
                                self.data_frames.append(data)
                        else:
                            # Potential data frame
                            self.msg_types['data'] += 1
                            self.data_frames.append(data)
                            
                    except json.JSONDecodeError:
                        pass
                        
        return len(self.frames)
        
    def analyze_schema(self):
        """Analyze field structure from data frames"""
        for frame in self.data_frames:
            self._analyze_object(frame)
            
    def _analyze_object(self, obj: Dict, prefix: str = ''):
        """Recursively analyze object structure"""
        if not isinstance(obj, dict):
            return
            
        for key, value in obj.items():
            field_path = f"{prefix}.{key}" if prefix else key
            field_info = self.field_schema[field_path]
            field_info['count'] += 1
            
            # Determine type
            if value is None:
                field_info['nullable'] = True
                field_info['types'].add('null')
            elif isinstance(value, bool):
                field_info['types'].add('boolean')
            elif isinstance(value, int):
                field_info['types'].add('integer')
            elif isinstance(value, float):
                field_info['types'].add('number')
            elif isinstance(value, str):
                field_info['types'].add('string')
                if len(field_info['samples']) < 3:
                    field_info['samples'].append(value[:100])
            elif isinstance(value, list):
                field_info['types'].add('array')
                if value and isinstance(value[0], dict):
                    self._analyze_object(value[0], f"{field_path}[]")
            elif isinstance(value, dict):
                field_info['types'].add('object')
                self._analyze_object(value, field_path)
                
    def generate_schema(self) -> Dict:
        """Generate JSON Schema from analysis"""
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "BoltOdds Message",
            "oneOf": []
        }
        
        # Connection messages
        if 'socket_connected' in self.msg_types or 'ping' in self.msg_types:
            schema["oneOf"].append({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["socket_connected", "ping", "pong"]
                    }
                },
                "required": ["action"]
            })
            
        # Data messages (if any)
        if self.data_frames:
            data_schema = {
                "type": "object",
                "properties": {},
                "required": []
            }
            
            # Build properties from field analysis
            for field_path, info in self.field_schema.items():
                if '.' not in field_path and '[]' not in field_path:
                    prop = {}
                    types = list(info['types'])
                    
                    if len(types) == 1:
                        prop['type'] = types[0]
                    else:
                        prop['type'] = types
                        
                    if info['samples']:
                        prop['examples'] = info['samples']
                        
                    data_schema['properties'][field_path] = prop
                    
                    # Mark as required if present in most frames
                    if info['count'] > len(self.data_frames) * 0.8:
                        data_schema['required'].append(field_path)
                        
            if data_schema['properties']:
                schema["oneOf"].append(data_schema)
                
        return schema
        
    def get_samples(self, count: int = 10) -> List[Dict]:
        """Get sample data frames"""
        samples = []
        
        # Get diverse samples
        seen_structures = set()
        
        for frame in self.frames:
            data = frame.get('data', {})
            
            # Skip pings and connection messages
            if data.get('action') in ['ping', 'socket_connected']:
                continue
                
            # Create structure signature
            structure = json.dumps(sorted(data.keys()))
            if structure not in seen_structures:
                seen_structures.add(structure)
                samples.append(data)
                
                if len(samples) >= count:
                    break
                    
        return samples
        
    def generate_report(self) -> Dict:
        """Generate analysis report"""
        report = {
            'total_frames': len(self.frames),
            'message_types': dict(self.msg_types),
            'data_frames_count': len(self.data_frames),
            'unique_fields': len(self.field_schema),
            'samples': self.get_samples(10),
            'field_summary': {}
        }
        
        # Summarize top-level fields
        for field, info in self.field_schema.items():
            if '.' not in field and '[]' not in field:
                report['field_summary'][field] = {
                    'count': info['count'],
                    'types': list(info['types']),
                    'nullable': info['nullable'],
                    'samples': info['samples'][:2]
                }
                
        return report

def main():
    """Run analysis"""
    analyzer = FrameAnalyzer()
    
    print("Loading frames...")
    frame_count = analyzer.load_frames()
    print(f"Loaded {frame_count} frames")
    
    print("Analyzing schema...")
    analyzer.analyze_schema()
    
    # Generate outputs
    report = analyzer.generate_report()
    schema = analyzer.generate_schema()
    
    # Save report
    output_dir = Path('docs/bolt_teardown')
    output_dir.mkdir(exist_ok=True, parents=True)
    
    with open(output_dir / 'analysis_report.json', 'w') as f:
        json.dump(report, f, indent=2)
        
    with open(output_dir / 'SCHEMA.json', 'w') as f:
        json.dump(schema, f, indent=2)
        
    # Print summary
    print("\n" + "="*60)
    print("ANALYSIS SUMMARY")
    print("="*60)
    print(f"Total frames: {report['total_frames']}")
    print(f"Data frames: {report['data_frames_count']}")
    print(f"Message types: {report['message_types']}")
    
    if report['field_summary']:
        print("\nField Analysis:")
        for field, info in report['field_summary'].items():
            print(f"  {field}: {info['types']} ({info['count']} occurrences)")
    else:
        print("\nNo data fields found (only control messages)")
        
    print(f"\nOutputs saved to {output_dir}")
    
    return report

if __name__ == '__main__':
    report = main()
    
    # Exit with status based on data frames
    sys.exit(0 if report['data_frames_count'] > 0 else 1)