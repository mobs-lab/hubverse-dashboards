"""
Manifest Manager for Hubverse Dashboard Incremental Updates

Tracks file changes in data directories to enable incremental processing.
"""

import json
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple

logger = logging.getLogger(__name__)


class ManifestManager:
    """
    Manages the manifest of data files to detect changes between runs.
    """

    def __init__(self, project_root: Path, manifest_path: Path = None):
        self.project_root = project_root
        if manifest_path:
            self.manifest_path = manifest_path
        else:
            self.manifest_path = self.project_root / "intermediates" / "manifest.json"
        
        self.manifest = self._load_manifest()
        self.current_state = {}

    def _load_manifest(self) -> dict:
        """Load existing manifest or return empty structure."""
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load manifest: {e}. Starting fresh.")
        
        return {
            "last_run": None,
            "files": {}  # path -> checksum
        }

    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate MD5 checksum of a file."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception:
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
            if file_path.is_file():
                try:
                    rel_path = str(file_path.relative_to(self.project_root))
                    checksum = self._calculate_checksum(file_path)
                    state[rel_path] = checksum
                except ValueError:
                    # Should not happen if directory is inside project_root
                    continue
        return state

    def check_changes(self, target_data_path: Path, model_output_path: Path) -> dict:
        """
        Scan current data directories and compare with manifest to detect changes.
        
        Returns:
            dict containing:
            - target_data_changed (bool)
            - new_model_files (List[str])
            - modified_model_files (List[str])
            - deleted_model_files (List[str])
        """
        logger.info("Scanning for data changes...")
        
        # Scan current state
        # Target data: track everything (csv, parquet)
        current_target_state = self.scan_directory(target_data_path)
        
        # Model output: track csv, parquet (and compressed versions)
        # We scan recursively
        current_model_state = self.scan_directory(model_output_path, "**/*")
        # Filter for relevant extensions if needed, but scanning all files is safer for now
        
        # Combine into full current state for storage later
        self.current_state = {**current_target_state, **current_model_state}
        
        previous_files = self.manifest.get("files", {})
        
        # 1. Check Target Data Changes
        target_changed = False
        target_files_current = set(current_target_state.keys())
        target_files_prev = {k for k in previous_files if k.startswith(str(target_data_path.relative_to(self.project_root)))}
        
        if target_files_current != target_files_prev:
            target_changed = True
            logger.info("Target data file list changed.")
        else:
            # Check content of existing files
            for f in target_files_current:
                if current_target_state[f] != previous_files.get(f):
                    target_changed = True
                    logger.info(f"Target data file modified: {f}")
                    break
        
        # 2. Check Model Output Changes
        new_models = []
        modified_models = []
        
        model_files_current = set(current_model_state.keys())
        model_files_prev = {k for k in previous_files if k.startswith(str(model_output_path.relative_to(self.project_root)))}
        
        # New files
        for f in model_files_current - model_files_prev:
            new_models.append(f)
            
        # Modified files
        for f in model_files_current.intersection(model_files_prev):
            if current_model_state[f] != previous_files[f]:
                modified_models.append(f)
                
        # Deleted files (we might not need to track this strictly for incremental logic, 
        # but good to know)
        deleted_models = list(model_files_prev - model_files_current)
        
        changes = {
            "target_data_changed": target_changed,
            "new_model_files": new_models,
            "modified_model_files": modified_models,
            "deleted_model_files": deleted_models
        }
        
        if target_changed:
            logger.info("  [!] Target data has changes.")
        if new_models:
            logger.info(f"  [!] Found {len(new_models)} new model output files.")
        if modified_models:
            logger.info(f"  [!] Found {len(modified_models)} modified model output files.")
            
        return changes

    def save(self):
        """Save current state to manifest."""
        self.manifest["files"] = self.current_state
        self.manifest["last_run"] = str(pd.Timestamp.now(tz="UTC")) if 'pd' in globals() else "now"
        
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.manifest_path, "w") as f:
            json.dump(self.manifest, f, indent=2)
        logger.info(f"Manifest saved to {self.manifest_path}")
