"""
Manifest Manager for Hubverse Dashboard Incremental Updates

Tracks file changes in data directories to enable incremental processing.
Organizes files by domain (target-data, model-output, auxiliary-data) for efficient change detection.
"""

import json
import hashlib
import logging
from pathlib import Path
from typing import Dict
from datetime import datetime

logger = logging.getLogger(__name__)


class ManifestManager:
    """
    Manages the manifest of data files to detect changes between runs.
    
    Important: The manifest tracks RAW source files using checksums.
    - When a file's checksum changes, we know the SOURCE data changed
    - The DataProcessor then loads and processes both old (from intermediates) and new (from files) data
    - Comparison happens on PROCESSED data (after column renaming, as_of shifting, etc.)
    - This ensures we're comparing apples-to-apples while detecting byte-level changes in sources
    
    Manifest Structure:
    {
        "version": "2.0",
        "last_run": "ISO datetime string",
        "domains": {
            "target_data": {
                "files": {"relative/path": "checksum"},
                "last_modified": "ISO datetime"
            },
            "model_output": {
                "by_model": {
                    "model_name": {
                        "files": ["file1", "file2"],
                        "last_modified": "ISO datetime"
                    }
                },
                "last_modified": "ISO datetime"
            },
            "auxiliary_data": {
                "files": {"relative/path": "checksum"},
                "last_modified": "ISO datetime"
            }
        }
    }
    """

    def __init__(self, project_root: Path, manifest_path: Path = None):
        """
        Initialize the ManifestManager.

        Loads an existing manifest from disk (if available) and prepares an
        empty ``current_state`` dictionary to accumulate scan results.

        Args:
            project_root: Root directory of the project. All relative file
                paths stored in the manifest are resolved against this root.
            manifest_path: Explicit path for the manifest JSON file. When
                ``None``, defaults to ``<project_root>/intermediates/manifest.json``.
        """
        self.project_root = project_root
        if manifest_path:
            self.manifest_path = manifest_path
        else:
            self.manifest_path = self.project_root / "intermediates" / "manifest.json"
        
        self.manifest = self._load_manifest()
        self.current_state = {"target_data": {}, "model_output": {}, "auxiliary_data": {}}

    def _load_manifest(self) -> dict:
        """
        Load an existing manifest from disk or return an empty structure.

        If the manifest file does not exist or cannot be parsed, a fresh
        manifest is created via :meth:`_create_empty_manifest`.

        Returns:
            dict: The loaded manifest dictionary, or a new empty manifest.
        """
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, "r") as f:
                    manifest = json.load(f)
                    
                    return manifest
            except Exception as e:
                logger.warning(f"Failed to load manifest: {e}. Starting fresh.")
        
        return self._create_empty_manifest()
    
    def _create_empty_manifest(self) -> dict:
        """
        Create an empty manifest with the current version 2.0 structure.

        The returned dictionary contains top-level metadata (``version``,
        ``last_run``) and a ``domains`` section with placeholders for
        ``target_data``, ``model_output``, and ``auxiliary_data``.

        Returns:
            dict: A blank manifest dictionary ready to be populated.
        """
        return {
            "version": "2.0",
            "last_run": None,
            "domains": {
                "target_data": {
                    "files": {},
                    "last_modified": None
                },
                "model_output": {
                    "files": {},
                    "by_model": {},
                    "last_modified": None
                },
                "auxiliary_data": {
                    "files": {},
                    "last_modified": None
                }
            }
        }

    def _calculate_checksum(self, file_path: Path) -> str:
        """
        Calculate the MD5 checksum of a file.

        Reads the file in 4 KB chunks to keep memory usage low for large
        data files.

        Args:
            file_path: Absolute path to the file to checksum.

        Returns:
            str: Hex-encoded MD5 digest, or an empty string if the file
            cannot be read.
        """
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.debug(f"Failed to checksum {file_path}: {e}")
            return ""

    def scan_directory(self, directory: Path, glob_pattern: str = "**/*") -> Dict[str, str]:
        """
        Scan a directory and calculate checksums for all matching files.
        Returns: Dict[relative_path_str, checksum]
        """
        state = {}
        if not directory.exists():
            return state

        for file_path in directory.glob(glob_pattern):
            if file_path.is_file() and not file_path.name.startswith('.'):
                try:
                    rel_path = str(file_path.relative_to(self.project_root))
                    checksum = self._calculate_checksum(file_path)
                    state[rel_path] = checksum
                except ValueError:
                    # Should not happen if directory is inside project_root
                    continue
        return state
    
    def scan_model_output_by_model(
        self, 
        model_output_path: Path, 
        configured_models: list = None
    ) -> Dict[str, Dict[str, str]]:
        """
        Scan model output directory organized by model subdirectories.
        
        Only scans directories for models that are configured in config.yaml.
        
        Args:
            model_output_path: Path to model-output directory
            configured_models: List of model names from config (optional)
        
        Returns: 
            Dict[model_name, Dict[file_path, checksum]]
        """
        by_model = {}
        
        if not model_output_path.exists():
            return by_model
        
        # If no configured models provided, scan all directories (fallback)
        if not configured_models:
            logger.warning("No configured models provided, scanning all model directories")
            model_dirs_to_scan = [d for d in model_output_path.iterdir() 
                                 if d.is_dir() and not d.name.startswith('.')]
        else:
            # Only scan configured model directories
            model_dirs_to_scan = []
            for model_name in configured_models:
                model_dir = model_output_path / model_name
                if model_dir.exists() and model_dir.is_dir():
                    model_dirs_to_scan.append(model_dir)
                else:
                    logger.debug(f"Configured model '{model_name}' directory not found, skipping")
        
        # Scan each model directory
        for model_dir in model_dirs_to_scan:
            model_name = model_dir.name
            by_model[model_name] = {}
            
            # Scan files within this model directory
            for file_path in model_dir.glob("**/*"):
                if file_path.is_file() and not file_path.name.startswith('.'):
                    try:
                        rel_path = str(file_path.relative_to(self.project_root))
                        checksum = self._calculate_checksum(file_path)
                        by_model[model_name][rel_path] = checksum
                    except ValueError:
                        continue
        
        return by_model

    def check_changes(
        self, 
        target_data_path: Path, 
        model_output_path: Path, 
        auxiliary_data_path: Path = None,
        configured_models: list = None
    ) -> dict:
        """
        Scan current data directories and compare with manifest to detect changes.
        
        Uses domain-based organization for efficient change detection.
        
        Args:
            target_data_path: Path to target-data directory
            model_output_path: Path to model-output directory
            auxiliary_data_path: Optional path to auxiliary-data directory
            configured_models: List of model names from config to scan
        
        Returns:
            dict containing:
                - target_data_changed (bool): Whether any target data changed
                - target_data_details (dict): Details about target data changes
                - model_output_changed (bool): Whether any model output changed
                - model_output_details (dict): Details by model
                - auxiliary_data_changed (bool): Whether auxiliary data changed
                - auxiliary_data_details (dict): Details about auxiliary changes
        """
        logger.info("Scanning for data changes...")
        
        domains = self.manifest.get("domains", {})
        
        # 1. Check Target Data Changes
        logger.info("  Scanning target-data...")
        target_result = self._check_target_data_changes(target_data_path, domains.get("target_data", {}))
        
        # 2. Check Model Output Changes (organized by model)
        logger.info("  Scanning model-output...")
        if configured_models:
            logger.info(f"  Scanning configured models: {', '.join(configured_models[:5])}{'...' if len(configured_models) > 5 else ''}")
        model_result = self._check_model_output_changes(
            model_output_path, 
            domains.get("model_output", {}),
            configured_models
        )
        
        # 3. Check Auxiliary Data Changes
        auxiliary_result = {"changed": False, "details": {}}
        if auxiliary_data_path and auxiliary_data_path.exists():
            logger.info("  Scanning auxiliary-data...")
            auxiliary_result = self._check_auxiliary_data_changes(auxiliary_data_path, domains.get("auxiliary_data", {}))
        
        # Summarize changes
        changes = {
            "target_data_changed": target_result["changed"],
            "target_data_details": target_result["details"],
            "model_output_changed": model_result["changed"],
            "model_output_details": model_result["details"],
            "auxiliary_data_changed": auxiliary_result["changed"],
            "auxiliary_data_details": auxiliary_result["details"],
        }
        
        # Log summary
        if target_result["changed"]:
            logger.info(f"  [!] Target data has changes: {target_result['summary']}")
        if model_result["changed"]:
            logger.info(f"  [!] Model output has changes: {model_result['summary']}")
        if auxiliary_result["changed"]:
            logger.info(f"  [!] Auxiliary data has changes: {auxiliary_result['summary']}")
        
        if not (target_result["changed"] or model_result["changed"] or auxiliary_result["changed"]):
            logger.info("  [OK] No data changes detected.")
            
        return changes
    
    def _check_target_data_changes(self, target_data_path: Path, previous_domain: dict) -> dict:
        """
        Check for changes in target data files.

        Compares current file checksums against the previous manifest domain
        to identify new, modified, and deleted files.

        Args:
            target_data_path: Path to the ``target-data`` directory.
            previous_domain: The ``target_data`` domain dict from the last
                saved manifest, containing a ``files`` mapping.

        Returns:
            dict: Result with keys ``changed`` (bool), ``details`` (dict of
            ``new_files``, ``modified_files``, ``deleted_files`` lists), and
            ``summary`` (human-readable string).
        """
        current_files = self.scan_directory(target_data_path)
        previous_files = previous_domain.get("files", {})
        
        self.current_state["target_data"] = current_files
        
        current_set = set(current_files.keys())
        previous_set = set(previous_files.keys())
        
        new_files = list(current_set - previous_set)
        deleted_files = list(previous_set - current_set)
        modified_files = [
            f for f in current_set.intersection(previous_set)
            if current_files[f] != previous_files[f]
        ]
        
        changed = bool(new_files or deleted_files or modified_files)
        
        summary = []
        if new_files:
            summary.append(f"{len(new_files)} new file(s)")
        if modified_files:
            summary.append(f"{len(modified_files)} modified file(s)")
        if deleted_files:
            summary.append(f"{len(deleted_files)} deleted file(s)")
        
        return {
            "changed": changed,
            "details": {
                "new_files": new_files,
                "modified_files": modified_files,
                "deleted_files": deleted_files,
            },
            "summary": ", ".join(summary) if summary else "no changes"
        }
    
    def _check_model_output_changes(
        self, 
        model_output_path: Path, 
        previous_domain: dict,
        configured_models: list = None
    ) -> dict:
        """
        Check for changes in model output files, organized by model.

        Scans each configured model's subdirectory and compares current file
        checksums with the previous manifest to detect new models, deleted
        models, and per-model file additions or modifications.

        Args:
            model_output_path: Path to the ``model-output`` directory.
            previous_domain: The ``model_output`` domain dict from the last
                saved manifest, containing a ``by_model`` mapping.
            configured_models: List of model names to scan. If ``None``, all
                model subdirectories are scanned.

        Returns:
            dict: Result with keys ``changed`` (bool), ``details`` (dict of
            ``new_models``, ``deleted_models``, ``changes_by_model``), and
            ``summary`` (human-readable string).
        """
        current_by_model = self.scan_model_output_by_model(model_output_path, configured_models)
        previous_by_model = previous_domain.get("by_model", {})
        
        self.current_state["model_output"] = {
            "by_model": current_by_model
        } 
        
        # Detect changes by model
        changes_by_model = {}
        current_models = set(current_by_model.keys())
        previous_models = set(previous_by_model.keys())
        
        new_models = list(current_models - previous_models)
        deleted_models = list(previous_models - current_models)
        
        for model in current_models:
            current_model_files = current_by_model[model]
            # Extract checksums dict from nested structure
            # Manifest file structure: {model: {"files": [...], "checksums": {...}, "last_modified": ...}}
            previous_model_data = previous_by_model.get(model, {})
            previous_model_files = previous_model_data.get("checksums", {}) if isinstance(previous_model_data, dict) else {}
            
            current_set = set(current_model_files.keys())
            previous_set = set(previous_model_files.keys())
            
            new_files = list(current_set - previous_set)
            modified_files = [
                f for f in current_set.intersection(previous_set)
                if current_model_files[f] != previous_model_files[f]
            ]
            
            if new_files or modified_files:
                changes_by_model[model] = {
                    "new_files": new_files,
                    "modified_files": modified_files,
                }
        
        changed = bool(new_models or deleted_models or changes_by_model)
        
        summary = []
        if new_models:
            summary.append(f"{len(new_models)} new model(s)")
        if len(changes_by_model) > 0:
            total_new = sum(len(c["new_files"]) for c in changes_by_model.values())
            total_modified = sum(len(c["modified_files"]) for c in changes_by_model.values())
            if total_new > 0:
                summary.append(f"{total_new} new file(s)")
            if total_modified > 0:
                summary.append(f"{total_modified} modified file(s)")
        
        return {
            "changed": changed,
            "details": {
                "new_models": new_models,
                "deleted_models": deleted_models,
                "changes_by_model": changes_by_model,
            },
            "summary": ", ".join(summary) if summary else "no changes"
        }
    
    def _check_auxiliary_data_changes(self, auxiliary_data_path: Path, previous_domain: dict) -> dict:
        """
        Check for changes in auxiliary data files.

        Compares current file checksums against the previous manifest domain
        to identify new, modified, and deleted files.

        Args:
            auxiliary_data_path: Path to the ``auxiliary-data`` directory.
            previous_domain: The ``auxiliary_data`` domain dict from the last
                saved manifest, containing a ``files`` mapping.

        Returns:
            dict: Result with keys ``changed`` (bool), ``details`` (dict of
            ``new_files``, ``modified_files``, ``deleted_files`` lists), and
            ``summary`` (human-readable string).
        """
        current_files = self.scan_directory(auxiliary_data_path)
        previous_files = previous_domain.get("files", {})
        
        self.current_state["auxiliary_data"] = current_files
        
        current_set = set(current_files.keys())
        previous_set = set(previous_files.keys())
        
        new_files = list(current_set - previous_set)
        deleted_files = list(previous_set - current_set)
        modified_files = [
            f for f in current_set.intersection(previous_set)
            if current_files[f] != previous_files[f]
        ]
        
        changed = bool(new_files or deleted_files or modified_files)
        
        summary = []
        if new_files:
            summary.append(f"{len(new_files)} new file(s)")
        if modified_files:
            summary.append(f"{len(modified_files)} modified file(s)")
        if deleted_files:
            summary.append(f"{len(deleted_files)} deleted file(s)")
        
        return {
            "changed": changed,
            "details": {
                "new_files": new_files,
                "modified_files": modified_files,
                "deleted_files": deleted_files,
            },
            "summary": ", ".join(summary) if summary else "no changes"
        }
    
    def update_state_from_directories(
        self,
        target_data_path: Path,
        model_output_path: Path,
        auxiliary_data_path: Path = None,
        configured_models: list = None
    ):
        """
        Scan and update current state from directories without comparing to previous state.
        
        Use this method after a from-scratch build to populate the manifest with current file state.
        
        Args:
            target_data_path: Path to target-data directory
            model_output_path: Path to model-output directory
            auxiliary_data_path: Optional path to auxiliary-data directory
            configured_models: List of model names from config to scan
        """
        logger.info("Updating manifest state from current directories...")
        
        # Scan target data
        if target_data_path.exists():
            self.current_state["target_data"] = self.scan_directory(target_data_path)
        
        # Scan model output
        if model_output_path.exists():
            by_model = self.scan_model_output_by_model(model_output_path, configured_models)
            self.current_state["model_output"] = {
                "by_model": by_model
            }
        
        # Scan auxiliary data
        if auxiliary_data_path and auxiliary_data_path.exists():
            self.current_state["auxiliary_data"] = self.scan_directory(auxiliary_data_path)
        
        logger.info("  [OK] Manifest state updated")

    def save(self):
        """
        Persist the current scan state to the manifest JSON file.

        Merges ``current_state`` into the manifest structure, updates
        ``last_run`` and per-domain ``last_modified`` timestamps, then
        writes the result to :attr:`manifest_path`. Parent directories
        are created automatically if they do not exist.
        """
        now = datetime.now().isoformat()
        
        # Update manifest with current state
        self.manifest["last_run"] = now
        self.manifest["version"] = "2.0"
        
        # Update target data domain
        if "target_data" in self.current_state:
            self.manifest["domains"]["target_data"]["files"] = self.current_state["target_data"]
            if self.current_state["target_data"]:
                self.manifest["domains"]["target_data"]["last_modified"] = now
        
        # Update model output domain
        if "model_output" in self.current_state:
            model_data = self.current_state["model_output"]
            
            if "files_flat" in model_data:
                self.manifest["domains"]["model_output"]["files"] = model_data["files_flat"]
            
            if "by_model" in model_data:
                # Update by_model structure
                by_model_manifest = {}
                for model_name, files in model_data["by_model"].items():
                    by_model_manifest[model_name] = {
                        "files": list(files.keys()),
                        "checksums": files,
                        "last_modified": now
                    }
                self.manifest["domains"]["model_output"]["by_model"] = by_model_manifest
            
            if model_data.get("files_flat") or model_data.get("by_model"):
                self.manifest["domains"]["model_output"]["last_modified"] = now
        
        # Update auxiliary data domain
        if "auxiliary_data" in self.current_state:
            self.manifest["domains"]["auxiliary_data"]["files"] = self.current_state["auxiliary_data"]
            if self.current_state["auxiliary_data"]:
                self.manifest["domains"]["auxiliary_data"]["last_modified"] = now
        
        # Write to file
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.manifest_path, "w") as f:
            json.dump(self.manifest, f, indent=2)
        
        logger.info(f"Manifest saved to {self.manifest_path}")
