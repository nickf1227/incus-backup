#!/usr/bin/env python3
import subprocess
import json
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from collections import defaultdict

# ==========================
# Configuration Variables
# ==========================
DEBUG = False                               # Enable/disable debug output.
BACKUP_RETENTION_DAYS = 14                 # Days to retain backup files
BACKUP_DIR = "/mnt/ice/incus-vms/"         # Directory for exported backups
LOG_FILE = "/var/log/incus-backup-nickf.log" # Log file path
STORAGE_POOL = "default"                   # Name of the Incus storage pool
PROJECT = "default"                        # Incus project name
# ==========================

# Set up logging
logger = logging.getLogger("incus_backup")
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)

# Create formatter that includes a timestamp.
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.DEBUG if DEBUG else logging.INFO)
logger.addHandler(console_handler)

# File handler with rotation (max 5 MB per file, 3 backup files)
try:
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
except Exception as e:
    logger.error(f"Error setting up file handler for log file {LOG_FILE}: {e}")

def check_backup_dir(path):
    """Check if the backup directory exists; if not, try to create it."""
    if not os.path.isdir(path):
        logger.debug(f"Backup directory '{path}' does not exist. Attempting to create it.")
        try:
            os.makedirs(path, exist_ok=True)
            logger.debug(f"Successfully created backup directory '{path}'.")
        except Exception as e:
            raise RuntimeError(f"Failed to create backup directory '{path}': {e}")

def run_command(command, description="", capture=True):
    """
    Run a subprocess command.
    If capture is True, it captures stdout and stderr.
    Logs debug output and errors.
    """
    logger.debug(f"Running command: {' '.join(command)} {description}")
    try:
        result = subprocess.run(command, capture_output=capture, text=True, check=True)
        logger.debug(f"Command output: {result.stdout.strip()}")
        return result
    except subprocess.CalledProcessError as e:
        err_msg = f"Error during command '{' '.join(command)}': {e.stderr.strip() if e.stderr else e}"
        logger.error(err_msg)
        raise RuntimeError(err_msg)

def get_vm_names():
    """
    Runs 'incus list' and parses the output to extract unique VM names.
    Assumes the output is a table where the first column is NAME.
    """
    try:
        result = run_command(["incus", "list"], description="listing VMs")
    except RuntimeError as e:
        logger.error(f"Error running 'incus list': {e}")
        return []

    vm_names = set()
    for line in result.stdout.splitlines():
        logger.debug(f"Processing line: {line}")
        if line.startswith("|"):
            parts = [part.strip() for part in line.strip().split("|")]
            if len(parts) > 1 and parts[1] != "NAME" and parts[1]:
                logger.debug(f"Found VM name: {parts[1]}")
                vm_names.add(parts[1])
    vm_list = list(vm_names)
    logger.debug(f"Total VMs found: {vm_list}")
    return vm_list

def get_vm_block_volumes(storage_pool, project="default"):
    """Returns a dictionary mapping VM names to their custom block volumes in the specified project."""
    try:
        result = run_command(
            ["incus", "storage", "volume", "list", storage_pool, "--all-projects", "--format", "json"],
            description=f"listing storage volumes in pool {storage_pool}"
        )
        volumes = json.loads(result.stdout)
    except Exception as e:
        logger.error(f"Failed to get storage volumes: {e}")
        return defaultdict(list)

    vm_volumes = defaultdict(list)
    for vol in volumes:
        if (
            vol.get("project") == project
            and vol.get("type") == "custom"
            and vol.get("content_type") == "block"
        ):
            for used_by in vol.get("used_by", []):
                if used_by.startswith("/1.0/instances/"):
                    vm_name = used_by.split("/")[-1]
                    vm_volumes[vm_name].append(vol["name"])
    return vm_volumes

def format_duration(duration: timedelta) -> str:
    """
    Format a timedelta into a human-readable string: seconds, minutes, or hours.
    """
    total_seconds = duration.total_seconds()
    if total_seconds < 60:
        return f"{total_seconds:.2f} seconds"
    elif total_seconds < 3600:
        minutes = total_seconds / 60
        return f"{minutes:.2f} minutes"
    else:
        hours = total_seconds / 3600
        return f"{hours:.2f} hours"


def export_vm(vm_name: str, backup_path: str) -> None:
    """
    Exports the VM to a file using the incus export command and calculates metrics.
    """
    logger.info(f"Exporting VM '{vm_name}' to file '{backup_path}'")
    try:
        start_time = datetime.now()
        run_command(
            ["incus", "export", vm_name, backup_path, "--optimized-storage", "--instance-only"],
            description=f"exporting VM {vm_name} to {backup_path}"
        )
        end_time = datetime.now()
        duration = end_time - start_time

        if os.path.exists(backup_path):
            size_bytes = os.path.getsize(backup_path)
            size_mb = size_bytes / (1024 * 1024)
            duration_str = format_duration(duration)
            speed = size_mb / duration.total_seconds() if duration.total_seconds() > 0 else float('inf')

            logger.info(
                f"Backup of {vm_name} completed in {duration_str}. "
                f"Size: {size_mb:.2f} MB. Speed: {speed:.2f} MB/s"
            )
        else:
            logger.error(f"Backup file {backup_path} not found after export")
    except Exception as e:
        logger.error(f"Error exporting VM {vm_name}: {e}")
        raise


def export_block_volume(storage_pool: str, volume_name: str, backup_path: str, project: str = None) -> None:
    """
    Exports a block storage volume to a file using incus storage volume export.
    """
    logger.info(f"Exporting block volume '{volume_name}' to '{backup_path}'")
    try:
        start_time = datetime.now()
        cmd = [
            "incus", "storage", "volume", "export",
            storage_pool,
            volume_name,
            backup_path,
            "--optimized-storage"
        ]
        if project:
            cmd.extend(["--project", project])
        run_command(cmd, description=f"exporting block volume {volume_name}")
        end_time = datetime.now()
        duration = end_time - start_time

        if os.path.exists(backup_path):
            size_bytes = os.path.getsize(backup_path)
            size_mb = size_bytes / (1024 * 1024)
            duration_str = format_duration(duration)
            speed = size_mb / duration.total_seconds() if duration.total_seconds() > 0 else float('inf')

            logger.info(
                f"Block volume {volume_name} exported in {duration_str}. "
                f"Size: {size_mb:.2f} MB. Speed: {speed:.2f} MB/s"
            )
        else:
            logger.error(f"Block volume backup {backup_path} not found after export")
    except Exception as e:
        logger.error(f"Error exporting block volume {volume_name}: {e}")
        raise

def prune_old_backups():
    """Removes backup files older than BACKUP_RETENTION_DAYS days"""
    logger.info(f"Pruning backups older than {BACKUP_RETENTION_DAYS} days")
    
    cutoff = datetime.now() - timedelta(days=BACKUP_RETENTION_DAYS)
    deleted_count = 0
    error_count = 0

    try:
        for filename in os.listdir(BACKUP_DIR):
            if not filename.endswith(".tar.gz"):
                continue
                
            try:
                timestamp_str = filename.split("-")[-1].split(".")[0]
                file_date = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                file_path = os.path.join(BACKUP_DIR, filename)
                
                if file_date < cutoff:
                    logger.debug(f"Deleting old backup: {filename}")
                    os.remove(file_path)
                    deleted_count += 1
                    
            except ValueError as e:
                error_count += 1
                logger.error(f"Invalid timestamp in filename {filename}: {e}")
            except Exception as e:
                error_count += 1
                logger.error(f"Error deleting {filename}: {e}")

        logger.info(f"Backup pruning complete. Deleted {deleted_count} files, {error_count} errors")

    except Exception as e:
        logger.error(f"Failed to prune backups: {e}")

def main():
    logger.info("Starting backup process for Incus VMs.")
    
    try:
        check_backup_dir(BACKUP_DIR)
        vm_names = get_vm_names()
        vm_block_volumes = get_vm_block_volumes(STORAGE_POOL, PROJECT)
        
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        for vm in vm_names:
            logger.info(f"Processing VM: {vm}")
            try:
                # Export VM
                vm_backup_path = f"{BACKUP_DIR.rstrip('/')}/{vm}-{timestamp}.tar.gz"
                export_vm(vm, vm_backup_path)
                
                # Export associated block volumes
                if vm in vm_block_volumes:
                    for volume_name in vm_block_volumes[vm]:
                        block_backup_path = f"{BACKUP_DIR.rstrip('/')}/{vm}-block-{volume_name}-{timestamp}.tar.gz"
                        export_block_volume(STORAGE_POOL, volume_name, block_backup_path, PROJECT)
                else:
                    logger.debug(f"No block volumes found for VM {vm}")
            except Exception as e:
                logger.error(f"Skipping {vm} due to error: {e}")
        
        prune_old_backups()
        
    except Exception as e:
        logger.error(f"Fatal error in main process: {e}")

if __name__ == "__main__":
    main()
