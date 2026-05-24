"""
YAML Generator
Transforms WB API JSON responses to wbopendata YAML schema v2.0.0
"""

from __future__ import annotations

import hashlib
import re
import logging
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import yaml

logger = logging.getLogger(__name__)


# Module-level YAML dumper config.
# Previously duplicated in both generate_indicators_yaml() and _write_yaml(),
# each registering yaml.add_representer per-call — which mutates global
# PyYAML state and added redundant work. Hoisted here so registration
# happens once at import time; idempotent for repeat-imports.

def _wrap_long_text(value: str) -> str:
    """Wrap strings >200 chars at width 120 for YAML readability."""
    if len(value) <= 200 or " " not in value:
        return value
    return textwrap.fill(value, width=120, break_long_words=False, break_on_hyphens=False)


def _str_representer(dumper, data):
    """Emit folded-block scalar (`>`) when wrapping introduces newlines."""
    wrapped = _wrap_long_text(data)
    if "\n" in wrapped:
        return dumper.represent_scalar("tag:yaml.org,2002:str", wrapped, style=">")
    return dumper.represent_scalar("tag:yaml.org,2002:str", wrapped)


yaml.add_representer(str, _str_representer, Dumper=yaml.SafeDumper)


class YAMLGenerator:
    """Generate YAML files from WB API data"""

    SCHEMA_VERSION = "2.0.0"
    GENERATOR_VERSION = SCHEMA_VERSION

    DEFAULT_FILENAMES = {
        "indicators": "_wbopendata_indicators.yaml",
        "sources": "_wbopendata_sources.yaml",
        "topics": "_wbopendata_topics.yaml",
    }

    def __init__(self, output_dir: Path | None = None, filenames: Dict[str, str] | None = None):
        """
        Initialize YAML generator

        Args:
            output_dir: Directory for output YAML files (default:
                       :func:`wb_api_tools.cache.get_cache_dir`, i.e.
                       ``~/.cache/wbopendata/`` on POSIX or
                       ``~/AppData/Local/wbopendata/`` on Windows; overridable
                       via the ``WBOPENDATA_YAML_DIR`` env var).
            filenames: Per-target filename override dict with keys
                       'indicators', 'sources', 'topics'. Missing keys fall
                       back to DEFAULT_FILENAMES. Lets update_metadata.py
                       honour config_update.yaml's yaml_output.*_file keys.
        """
        from .cache import get_cache_dir
        default_dir = get_cache_dir()
        self.output_dir = Path(output_dir) if output_dir else default_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        merged = dict(self.DEFAULT_FILENAMES)
        if filenames:
            merged.update({k: v for k, v in filenames.items() if v})
        self.filenames = merged
    
    def generate_all(self, indicators: List[Dict], sources: List[Dict], 
                     topics: List[Dict]) -> Dict[str, Path]:
        """
        Generate all three YAML files
        
        Returns:
            Dictionary mapping file type to output path
        """
        return {
            'indicators_file': self.generate_indicators_yaml(indicators),
            'sources_file': self.generate_sources_yaml(sources),
            'topics_file': self.generate_topics_yaml(topics)
        }
    
    def generate_indicators_yaml(self, indicators: List[Dict]) -> Path:
        """
        Generate _wbopendata_indicators.yaml
        
        Args:
            indicators: List of indicator dicts from API
        
        Returns:
            Path to generated YAML file
        """
        logger.info(f"Generating indicators YAML for {len(indicators)} indicators...")

        valid_indicators = []
        for ind in indicators:
            code = str(ind.get("id", "")).strip()
            # Numeric-only codes are treated as invalid for parity with Stata output.
            if not code or code.isdigit():
                continue
            valid_indicators.append(ind)
        
        # Build YAML structure (total_indicators set after dedup below)
        yaml_data = {
            '_metadata': {
                'version': self.SCHEMA_VERSION,
                'generated_at': datetime.utcnow().isoformat() + 'Z',
                'source': 'World Bank Open Data API',
                'total_indicators': 0,
                'compression': 'none',
                'encoding': 'UTF-8'
            },
            'indicators': {}
        }

        # Transform each indicator (dict key = code, so duplicates overwrite)
        for ind in valid_indicators:
            code = str(ind.get("id", "")).strip()

            yaml_data['indicators'][code] = {
                'code': code,
                'name': self._normalize_name(ind.get('name')),
                'source_id': self._extract_source_id(ind),
                'source_name': self._extract_source_name(ind),
                'topic_ids': self._extract_topic_ids(ind),
                'topic_names': self._extract_topic_names(ind),
                'description': self._clean_text(ind.get('sourceNote', '')),
                'unit': ind.get('unit', ''),
                'source_org': ind.get('sourceOrganization', ''),
                'note': ind.get('note', ''),
                'limited_data': False
            }

        # Set total_indicators to actual unique count (API may return duplicates)
        n_unique = len(yaml_data['indicators'])
        n_dupes = len(valid_indicators) - n_unique
        if n_dupes > 0:
            logger.warning(f"API returned {n_dupes} duplicate indicator codes (kept last occurrence)")
        yaml_data['_metadata']['total_indicators'] = n_unique

        # Calculate checksum: serialize the data BEFORE adding the checksum field,
        # using the same parameters as _write_yaml (but excluding the header).
        # This matches how the file will actually be written and allows validation
        # to recompute the checksum by temporarily removing the field.
        # Note: yaml dumper representer registered at module load (see top of file).
        import io

        # Serialize YAML content for checksum (without checksum field itself)
        yaml_content = io.StringIO()
        yaml.safe_dump(
            yaml_data,
            yaml_content,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
            width=10000,
        )

        # Compute checksum from YAML content (excluding header, matching validation logic)
        checksum = hashlib.sha256(yaml_content.getvalue().encode('utf-8')).hexdigest()

        # Now add the checksum to metadata
        yaml_data['_metadata']['checksum_sha256'] = checksum

        # Write to file (this will include the checksum field)
        output_file = self.output_dir / self.filenames["indicators"]
        self._write_yaml(yaml_data, output_file)
        
        logger.info(f"Generated {output_file} ({output_file.stat().st_size:,} bytes)")
        return output_file
    
    def generate_sources_yaml(self, sources: List[Dict]) -> Path:
        """Generate _wbopendata_sources.yaml"""
        logger.info(f"Generating sources YAML for {len(sources)} sources...")

        # Build YAML structure; total_sources set after empty-id filter
        # so the count matches what's actually written (was len(input) — over-counted
        # whenever the API returned records with missing IDs).
        yaml_data = {
            '_metadata': {
                'version': self.SCHEMA_VERSION,
                'generated_at': datetime.utcnow().isoformat() + 'Z',
                'total_sources': 0,
            },
            'sources': {}
        }

        n_empty_id = 0
        for src in sources:
            code = str(src.get('id', ''))
            if not code:
                n_empty_id += 1
                continue

            yaml_data['sources'][code] = {
                'code': code,
                'name': self._normalize_name(src.get('name')),
                'description': self._clean_text(src.get('description', '')),
                'url': src.get('url', ''),
                'data_availability': src.get('dataavailability', ''),
                'metadata_availability': src.get('metadataavailability', '')
            }

        n_unique = len(yaml_data['sources'])
        n_duplicates = len(sources) - n_empty_id - n_unique
        if n_empty_id > 0:
            logger.warning(f"Dropped {n_empty_id} source record(s) with empty id (kept {n_unique})")
        if n_duplicates > 0:
            logger.warning(f"API returned {n_duplicates} duplicate source code(s) (kept last occurrence)")
        yaml_data['_metadata']['total_sources'] = n_unique

        output_file = self.output_dir / self.filenames["sources"]
        self._write_yaml(yaml_data, output_file)
        
        logger.info(f"Generated {output_file} ({output_file.stat().st_size:,} bytes)")
        return output_file
    
    def generate_topics_yaml(self, topics: List[Dict]) -> Path:
        """Generate _wbopendata_topics.yaml"""
        logger.info(f"Generating topics YAML for {len(topics)} topics...")

        # Build YAML structure; total_topics set after empty-id filter
        # so the count matches what's actually written (mirrors the
        # generate_sources_yaml fix; was len(input) before).
        yaml_data = {
            '_metadata': {
                'version': self.SCHEMA_VERSION,
                'generated_at': datetime.utcnow().isoformat() + 'Z',
                'total_topics': 0,
            },
            'topics': {}
        }

        n_empty_id = 0
        for topic in topics:
            code = str(topic.get('id', ''))
            if not code:
                n_empty_id += 1
                continue

            yaml_data['topics'][code] = {
                'code': code,
                'name': self._normalize_name(topic.get('value')),
                'description': self._clean_text(topic.get('sourceNote', ''))
            }

        n_unique = len(yaml_data['topics'])
        n_duplicates = len(topics) - n_empty_id - n_unique
        if n_empty_id > 0:
            logger.warning(f"Dropped {n_empty_id} topic record(s) with empty id (kept {n_unique})")
        if n_duplicates > 0:
            logger.warning(f"API returned {n_duplicates} duplicate topic code(s) (kept last occurrence)")
        yaml_data['_metadata']['total_topics'] = n_unique

        output_file = self.output_dir / self.filenames["topics"]
        self._write_yaml(yaml_data, output_file)
        
        logger.info(f"Generated {output_file} ({output_file.stat().st_size:,} bytes)")
        return output_file
    
    def _write_yaml(self, data: Dict, output_file: Path):
        """Write YAML data to file with proper string formatting.

        String representer registered at module load — no per-call setup.
        """
        header = (
            f"# Generated by Python yaml_generator.py v{self.GENERATOR_VERSION} "
            f"(schema {self.SCHEMA_VERSION})\n"
        )

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(header)
            yaml.safe_dump(
                data,
                f,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
                width=10000,
            )
    
    def _extract_source_id(self, indicator: Dict) -> str:
        """Extract source ID from indicator"""
        source = indicator.get('source', {})
        if isinstance(source, dict):
            return str(source.get('id', ''))
        return ''
    
    def _extract_source_name(self, indicator: Dict) -> str:
        """Extract source name from indicator"""
        source = indicator.get('source', {})
        if isinstance(source, dict):
            return source.get('value', '')
        return ''
    
    def _extract_topic_ids(self, indicator: Dict) -> List[str]:
        """Extract topic IDs from indicator"""
        topics = indicator.get('topics', [])
        if not isinstance(topics, list):
            return []
        return [str(t.get('id', '')) for t in topics if isinstance(t, dict)]
    
    def _extract_topic_names(self, indicator: Dict) -> List[str]:
        """Extract topic names from indicator."""
        topics = indicator.get('topics', [])
        if not isinstance(topics, list):
            return []
        return [t.get('value', '').strip() for t in topics if isinstance(t, dict)]
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text - ensure single line
        Also applies light normalizations for known upstream typos.
        """
        if not text:
            return ''
        # Remove all line breaks and excessive whitespace
        text = ' '.join(text.split())
        # Normalize known content issues (e.g., "workers\ remittances" -> "workers' remittances")
        text = re.sub(r"workers\\\s+remittances", "workers' remittances", text, flags=re.IGNORECASE)
        # Common WB-API misspelling: "Millenium" -> "Millennium"
        text = re.sub(r"\bMilleni(?=um\b)", "Millenni", text, flags=re.IGNORECASE)
        return text.strip()

    def _normalize_name(self, name: str) -> str:
        """Trim + fix known upstream typos in metadata name fields.

        Lighter-weight than _clean_text (which also collapses internal
        whitespace + handles description-style text); applied to short
        identifier-style strings like source/topic/indicator names.
        """
        if not name:
            return ''
        name = name.strip()
        # Common WB-API misspelling: "Millenium" -> "Millennium"
        name = re.sub(r"\bMilleni(?=um\b)", "Millenni", name, flags=re.IGNORECASE)
        return name


if __name__ == '__main__':
    # Quick test
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Sample data
    sample_indicators = [
        {
            'id': 'SP.POP.TOTL',
            'name': 'Population, total',
            'source': {'id': '2', 'value': 'World Development Indicators'},
            'topics': [{'id': '8', 'value': 'Health'}],
            'sourceNote': 'Total population based on...',
            'unit': 'people'
        }
    ]
    
    sample_sources = [
        {'id': '2', 'name': 'World Development Indicators', 'description': 'Primary WB database'}
    ]
    
    sample_topics = [
        {'id': '8', 'value': 'Health', 'sourceNote': 'Health-related indicators'}
    ]
    
    generator = YAMLGenerator(output_dir=Path('test_output'))
    files = generator.generate_all(sample_indicators, sample_sources, sample_topics)
    
    print("\nGenerated YAML files:")
    for file_type, path in files.items():
        print(f"  - {file_type}: {path}")
