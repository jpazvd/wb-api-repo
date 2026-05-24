"""Main orchestrator for metadata updates (Pathway C)."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml

from .diff_analyzer import DiffAnalyzer
from .git_manager import GitManager
from .schema_validator import SchemaValidator
from .api_client import WBAPIClient
from .yaml_generator import YAMLGenerator

from .cache import get_cache_dir

# Packaged resources (config + schema) live inside the wheel.
_PKG_RESOURCES = Path(__file__).resolve().parent / "_resources"
DEFAULT_CONFIG_PATH = _PKG_RESOURCES / "config_update.yaml"
# Logs go to the user's cache dir, not site-packages.
LOG_DIR = get_cache_dir() / "logs"


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logfile = LOG_DIR / f"update_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(logfile),
        ],
    )


def _resolve_relative(path: Path, base: Path) -> Path:
    """Resolve a possibly-relative path against ``base``."""
    if path.is_absolute():
        return path
    return base / path


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load pipeline configuration from YAML.

    Relative ``config_path`` resolves against the user's cwd; the default
    points at the packaged config bundled in ``_resources/``.
    """
    config_path = _resolve_relative(config_path, Path.cwd())
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def resolve_output_paths(config: Dict[str, Any], override_dir: Path | None) -> Dict[str, Path]:
    """Resolve output paths for generated YAML files.

    Default base dir is the XDG cache dir (``wb_api_tools.cache.get_cache_dir()``)
    so a pip-installed user gets a working ``wb-api-tools sync`` out of the
    box. The config's ``yaml_output.base_dir`` is honored when set; relative
    values resolve against ``Path.cwd()`` (dev convenience).
    """
    yaml_cfg = config.get("yaml_output", {})
    base_dir_value = override_dir or yaml_cfg.get("base_dir") or get_cache_dir()
    base_dir = _resolve_relative(Path(base_dir_value), Path.cwd())
    return {
        "indicators": base_dir / yaml_cfg.get("indicators_file", "_wbopendata_indicators.yaml"),
        "sources": base_dir / yaml_cfg.get("sources_file", "_wbopendata_sources.yaml"),
        "topics": base_dir / yaml_cfg.get("topics_file", "_wbopendata_topics.yaml"),
    }


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Update wbopendata metadata from World Bank API"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to pipeline configuration file (default: config/config_update.yaml)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Override output directory for YAML files (default: config value)",
    )
    parser.add_argument(
        "--save-raw",
        action="store_true",
        help="Save raw API responses to JSON files",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--no-validate",
        dest="validate",
        action="store_false",
        help="Skip schema validation",
        default=True,
    )
    parser.add_argument(
        "--skip-diff",
        dest="diff",
        action="store_false",
        help="Skip diff summary",
        default=True,
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Stage and commit generated files",
    )
    parser.add_argument(
        "--tag",
        action="store_true",
        help="Create git tag when committing",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        config = load_config(Path(args.config))
    except Exception as exc:  # pragma: no cover - startup validation
        logging.error("Unable to load config: %s", exc)
        return 1

    logger.info("=" * 60)
    logger.info("wbopendata Metadata Update - Pathway C")
    logger.info("=" * 60)

    try:
        # Resolve paths and snapshot current state for diffing
        output_paths = resolve_output_paths(config, args.output_dir)
        base_dir = next(iter(output_paths.values())).parent
        base_dir.mkdir(parents=True, exist_ok=True)

        diff_analyzer = DiffAnalyzer()
        previous_keys = diff_analyzer.snapshot(output_paths)

        # Initialize clients
        wb_cfg = config.get("wb_api", {})
        api_timeout = wb_cfg.get("timeout", WBAPIClient.DEFAULT_TIMEOUT)
        per_page = wb_cfg.get("per_page", 10000)
        # wb_api.{base_url,retry_count,retry_delay} now wired into WBAPIClient
        # (was inert before — client read class constants regardless of config).
        api_base_url = wb_cfg.get("base_url")
        api_max_retries = wb_cfg.get("retry_count")
        api_retry_delay = wb_cfg.get("retry_delay")

        # Pull per-target filenames from config so yaml_output.*_file
        # overrides are honoured (was a no-op before — generator hardcoded).
        yaml_cfg = config.get("yaml_output", {})
        filenames = {
            "indicators": yaml_cfg.get("indicators_file"),
            "sources":    yaml_cfg.get("sources_file"),
            "topics":     yaml_cfg.get("topics_file"),
        }

        with WBAPIClient(
            timeout=api_timeout,
            base_url=api_base_url,
            max_retries=api_max_retries,
            retry_delay=api_retry_delay,
        ) as api_client:
            yaml_gen = YAMLGenerator(output_dir=base_dir, filenames=filenames)

            # Fetch data from WB API
            logger.info("\n[1/5] Fetching data from World Bank API...")
            indicators = api_client.fetch_indicators(per_page=per_page)
            sources = api_client.fetch_sources()
            topics = api_client.fetch_topics()

            if not indicators:
                logger.error("Failed to fetch indicators. Aborting.")
                return 1

            # Optionally save raw data
            if args.save_raw:
                logger.info("\n[2/5] Saving raw API responses...")
                raw_dir = _resolve_relative(
                    Path(config.get("data", {}).get("raw_dir", "data/raw")),
                    Path.cwd(),
                )
                api_client.save_raw_data(
                    {
                        "indicators": indicators,
                        "sources": sources,
                        "topics": topics,
                    },
                    output_dir=raw_dir,
                )
            else:
                logger.info("\n[2/5] Skipping raw data save (use --save-raw to enable)")

            # Generate YAML files
            logger.info("\n[3/5] Generating YAML files...")
            output_files = yaml_gen.generate_all(indicators, sources, topics)

        # Validate against schema
        if args.validate and config.get("validation", {}).get("enabled", True):
            logger.info("\n[4/5] Validating YAML outputs...")
            # Default schema is the packaged copy; config can override with
            # an absolute path or a path relative to cwd (dev convenience).
            schema_cfg = config.get("validation", {}).get("schema_path")
            if schema_cfg:
                schema_path = _resolve_relative(Path(schema_cfg), Path.cwd())
            else:
                schema_path = _PKG_RESOURCES / "schema_yaml_v2.json"
            validator = SchemaValidator(schema_path)
            for variant, path in output_files.items():
                result = validator.validate_yaml(path, variant)
                if not result["valid"]:
                    logger.error("Validation failed for %s (%s)", path, result["variant"])
                    return 1
            logger.info("Validation passed for all YAML files")
        else:
            logger.info("\n[4/5] Validation skipped")

        # Diff summary
        if args.diff and config.get("diff", {}).get("enabled", True):
            logger.info("\n[5/5] Diff summary vs previous files")
            # Map output_files keys (e.g., "indicators_file") to section names (e.g., "indicators")
            key_mapping = {
                "indicators_file": "indicators",
                "sources_file": "sources",
                "topics_file": "topics",
            }
            for file_key, new_path in output_files.items():
                section = key_mapping.get(file_key, file_key)
                old_keys = previous_keys.get(section, set())
                new_keys = diff_analyzer.load_keys(new_path, section=section)
                summary = diff_analyzer.summarize(old_keys, new_keys)
                logger.info(
                    "%s: before=%d after=%d added=%d removed=%d",
                    section,
                    summary["before"],
                    summary["after"],
                    summary["added"],
                    summary["removed"],
                )
        else:
            logger.info("\n[5/5] Diff step skipped")

        # Logging summary
        logger.info("\nSummary")
        logger.info("=" * 60)
        logger.info("✅ Successfully updated metadata!")
        logger.info("\nGenerated files:")
        for _, path in output_files.items():
            size = path.stat().st_size
            logger.info("  - %s: %s bytes", path.name, f"{size:,}")

        logger.info("\nStatistics:")
        logger.info("  - Indicators: %s", f"{len(indicators):,}")
        logger.info("  - Sources: %s", f"{len(sources):,}")
        logger.info("  - Topics: %s", f"{len(topics):,}")
        logger.info("=" * 60)

        # Optional Git automation
        git_cfg = config.get("git", {})
        should_commit = args.commit or git_cfg.get("auto_commit", False)
        if should_commit:
            manager = GitManager()
            if manager.has_changes(output_files.values()):
                manager.stage(output_files.values())
                version_label = datetime.utcnow().strftime("%Y.%m.%d")
                commit_msg = git_cfg.get(
                    "commit_message_template", "Update metadata: {version} ({date})"
                ).format(version=version_label, date=datetime.utcnow().strftime("%Y-%m-%d"))
                manager.commit(commit_msg)

                if args.tag or git_cfg.get("auto_tag", False):
                    tag_name = f"{git_cfg.get('tag_prefix', 'metadata-v')}{version_label}"
                    manager.tag(tag_name, message=commit_msg)
            else:
                logger.info("No file changes detected; skipping commit/tag")

        return 0

    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user")
        return 130

    except Exception as e:  # pragma: no cover - defensive catch
        logger.error("\n❌ Error: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
