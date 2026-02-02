"""
Data Fetcher for Hubverse Dashboard

This script handles fetching data from a remote Hubverse-compatible GitHub repository.
It supports both initial cloning and updating existing repositories.
"""

import shutil
import subprocess
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class DataFetcher:
    """
    Handles fetching and updating data from remote repositories.
    
    In development mode, caches data to development-mode-root/.data_cache
    In production mode, caches data to .data_cache
    """

    def __init__(self, project_root: Path, dev_mode: bool = False):
        self.project_root = project_root
        self.dev_mode = dev_mode

    def fetch_data(self, repo_url: str, local_cache_dir_name: str = None) -> tuple[bool, Path]:
        """
        Fetch data from a remote git repository and return success status and cache path.
        
        In dev mode: caches to development-mode-root/.data_cache
        In prod mode: caches to .data_cache (project root)
        
        Args:
            repo_url: URL of the git repository to fetch from
            local_cache_dir_name: Optional custom cache directory name (overrides dev_mode logic)
            
        Returns:
            tuple: (success: bool, cache_dir: Path)
        """
        if not repo_url:
            logger.error("No repository URL provided.")
            return False, None

        # Determine cache directory based on mode
        if local_cache_dir_name:
            cache_dir = self.project_root / local_cache_dir_name
        elif self.dev_mode:
            cache_dir = self.project_root / "development-mode-root" / ".data_cache"
            logger.info(f"Dev mode: caching to {cache_dir.relative_to(self.project_root)}")
        else:
            cache_dir = self.project_root / ".data_cache"
            logger.info("Prod mode: caching to .data_cache")
        
        try:
            # 1. Fetch/Update Cache
            if not self._update_or_clone_repo(repo_url, cache_dir):
                return False, None
            
            return True, cache_dir

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return False, None

    def sync_to_destination(self, source_dir_name_or_path: str, destination_root: Path, configured_models: list = None):
        """
        Copy data folders from source (cache) to destination.
        
        For model-output folder, only syncs subdirectories for configured models when provided.
        This prevents syncing unnecessary model data from the remote repository.
        
        Args:
            source_dir_name_or_path: Relative path to cache directory
            destination_root: Destination directory path
            configured_models: Optional list of model names to sync (for model-output only)
        """
        folders_to_copy = ["target-data", "model-output", "auxiliary-data"]
        
        # Handle both absolute paths and relative names
        if Path(source_dir_name_or_path).is_absolute():
            source_dir = Path(source_dir_name_or_path)
        else:
            source_dir = self.project_root / source_dir_name_or_path

        logger.info(
            f"Syncing data from cache ({source_dir_name_or_path}) to "
            f"{destination_root.relative_to(self.project_root) if destination_root != self.project_root else 'project root'}..."
        )

        for folder in folders_to_copy:
            src = source_dir / folder
            dst = destination_root / folder
            
            if not src.exists():
                logger.warning(f"  [!] Folder {folder} not found in remote repository")
                continue
            
            # Special handling for model-output when configured_models is provided
            if folder == "model-output" and configured_models:
                logger.info(f"  Syncing {folder} (configured models only)...")
                
                # Create destination directory if it doesn't exist
                dst.mkdir(parents=True, exist_ok=True)
                
                synced_count = 0
                for model_name in configured_models:
                    model_src = src / model_name
                    model_dst = dst / model_name
                    
                    if model_src.exists() and model_src.is_dir():
                        # Remove old model directory if exists
                        if model_dst.exists():
                            shutil.rmtree(model_dst)
                        # Copy model directory
                        shutil.copytree(model_src, model_dst)
                        synced_count += 1
                    else:
                        logger.debug(f"    Model '{model_name}' not found in remote repository")
                
                logger.info(f"    [OK] Synced {synced_count} model(s): {', '.join(configured_models)}")
            else:
                # Standard full folder sync for target-data and auxiliary-data
                logger.info(f"  Syncing {folder}...")
                if dst.exists():
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)
                logger.info(f"    [OK] Synced {folder}")

    def _update_or_clone_repo(self, repo_url: str, target_dir: Path) -> bool:
        """Helper to clone or update a repository"""
        try:
            if target_dir.exists() and (target_dir / ".git").exists():
                logger.info(f"Updating data from {repo_url}...")
                
                # Check URL mismatch
                current_url = subprocess.check_output(
                    ["git", "-C", str(target_dir), "remote", "get-url", "origin"], 
                    text=True
                ).strip()
                
                if current_url != repo_url:
                    logger.warning(f"Repo URL changed. Re-cloning...")
                    shutil.rmtree(target_dir)
                    return self._clone_repo(repo_url, target_dir)
                else:
                    subprocess.run(
                        ["git", "-C", str(target_dir), "pull", "origin", "main"], 
                        check=True, capture_output=True
                    )
                    logger.info("  [OK] Repository updated.")
                    return True
            else:
                if target_dir.exists():
                    shutil.rmtree(target_dir)
                return self._clone_repo(repo_url, target_dir)
        except subprocess.CalledProcessError as e:
            logger.error(f"Git operation failed: {e}")
            return False

    def _clone_repo(self, repo_url: str, target_dir: Path) -> bool:
        logger.info(f"Cloning data from {repo_url}...")
        try:
            subprocess.run(
                ["git", "clone", "--depth", "1", repo_url, str(target_dir)], 
                check=True, capture_output=True
            )
            logger.info(f"  [OK] Cloned to {target_dir}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Clone failed: {e}")
            return False
