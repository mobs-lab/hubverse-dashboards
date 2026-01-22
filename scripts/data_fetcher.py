"""
Data Fetcher for Hubverse Dashboard

This script handles fetching data from a remote Hubverse-compatible GitHub repository.
It supports both initial cloning and updating existing repositories.
"""

import shutil
import subprocess
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class DataFetcher:
    """
    Handles fetching and updating data from remote repositories.
    """

    def __init__(self, project_root: Path):
        self.project_root = project_root

    def fetch_data(self, repo_url: str, local_cache_dir_name: str = ".data_cache") -> bool:
        """
        Fetch data from a remote git repository and return the path to the cache.
        
        Args:
            repo_url: URL of the git repository to fetch from
            local_cache_dir_name: Name of the local directory to clone into (relative to project root)
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not repo_url:
            logger.error("No repository URL provided.")
            return False

        cache_dir = self.project_root / local_cache_dir_name
        
        try:
            # 1. Fetch/Update Cache
            if not self._update_or_clone_repo(repo_url, cache_dir):
                return False
            
            return True

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return False

    def sync_to_destination(self, source_dir_name: str, destination_root: Path):
        """Copy data folders from source (cache) to destination"""
        folders_to_copy = ["target-data", "model-output", "auxiliary-data"]
        source_dir = self.project_root / source_dir_name
        
        logger.info(f"Syncing data from cache ({source_dir_name}) to {destination_root.relative_to(self.project_root) if destination_root != self.project_root else 'project root'}...")

        for folder in folders_to_copy:
            src = source_dir / folder
            dst = destination_root / folder
            
            if src.exists():
                logger.info(f"  Syncing {folder}...")
                if dst.exists():
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)
                logger.info(f"    [OK] Synced {folder}")
            else:
                logger.warning(f"  [!] Folder {folder} not found in remote repository")

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
