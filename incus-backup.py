#!/usr/bin/env python3
import subprocess
import json
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

# ==========================
# Configuration Variables
# ==========================
DEBUG = True                               # Enable/disable debug output.
RETENTION_DAYS = 7                         # Number of days to retain snapshots (older snapshots will be pruned).
BACKUP_DIR = "/mnt/ice/incus-vms/"         # Directory to store the exported backup files.
LOG_FILE = "/var/log/incus-backup-nickf.log" # Log file path
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

def parse_iso_datetime(datetime_str):
    """Parse ISO 8601 datetime strings with optional fractional seconds, adjusting to 6 digits if needed."""
    if '.' in datetime_str and datetime_str.endswith('Z'):
        date_part, frac_part = datetime_str.split('.', 1)
        frac_part = frac_part.rstrip('Z')
        # Truncate or pad fractional part to 6 digits
        frac_adjusted = frac_part[:6].ljust(6, '0')  # Truncate to 6 digits, pad with zeros if shorter
        adjusted_str = f"{date_part}.{frac_adjusted}Z"
        return datetime.strptime(adjusted_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        # Handle case without fractional seconds
        return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")

def prune_old_snapshots(vm_name, retention_days):
    # ... [existing code] ...
    for snap in snapshots:
        snapshot_name = snap.get("name")
        created_str = snap.get("created_at")
        if not created_str:
            logger.warning(f"No creation date for snapshot '{snapshot_name}' in {vm_name}; skipping.")
            continue
        try:
            created_at = parse_iso_datetime(created_str)
        except Exception as e:
            logger.error(f"Error parsing creation date for snapshot '{snapshot_name}' in {vm_name}: {e}")
            continue

    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    logger.debug(f"Snapshot cutoff datetime (UTC) for {vm_name}: {cutoff.isoformat()}")

    for snap in snapshots:
        snapshot_name = snap.get("name")
        created_str = snap.get("created_at")
        if not created_str:
            logger.warning(f"No creation date for snapshot '{snapshot_name}' in {vm_name}; skipping.")
            continue
        try:
            created_at = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logger.error(f"Error parsing creation date for snapshot '{snapshot_name}' in {vm_name}: {e}")
            continue

        if created_at < cutoff:
            logger.info(f"Pruning snapshot '{snapshot_name}' of VM '{vm_name}' (created at {created_at.isoformat()} UTC)")
            try:
                run_command(["incus", "snapshot", "delete", vm_name, snapshot_name],
                            description=f"deleting snapshot {snapshot_name} for {vm_name}")
            except Exception as e:
                logger.error(f"Error deleting snapshot '{snapshot_name}' for {vm_name}: {e}")

def snapshot_vm(vm_name, snapshot_name):
    """
    Creates a snapshot of the specified VM using the 'incus snapshot create' command.
    """
    logger.info(f"Taking snapshot for VM '{vm_name}' with snapshot name '{snapshot_name}'")
    try:
        run_command(["incus", "snapshot", "create", vm_name, snapshot_name],
                    description=f"creating snapshot for {vm_name}")
    except Exception as e:
        logger.error(f"Error taking snapshot for {vm_name}: {e}")
        raise

def export_vm(vm_name, backup_path):
    """
    Exports the VM to a file using the incus export command and calculates metrics.
    """
    logger.info(f"Exporting VM '{vm_name}' to file '{backup_path}'")
    try:
        start_time = datetime.now()
        run_command(["incus", "export", vm_name, backup_path],
                    description=f"exporting VM {vm_name} to {backup_path}")
        end_time = datetime.now()
        
        # Calculate duration
        duration = end_time - start_time
        duration_sec = duration.total_seconds()
        
        # Get backup size and calculate speed
        if os.path.exists(backup_path):
            size_bytes = os.path.getsize(backup_path)
            size_mb = size_bytes / (1024 * 1024)  # Convert to MB
            speed = size_mb / duration_sec if duration_sec > 0 else float('inf')
            
            logger.info(
                f"Backup of {vm_name} completed in {duration_sec:.2f} seconds. "
                f"Size: {size_mb:.2f} MB. Speed: {speed:.2f} MB/s"
            )
        else:
            logger.error(f"Backup file {backup_path} not found after export")
            
    except Exception as e:
        logger.error(f"Error exporting VM {vm_name}: {e}")
        raise

def main():
    logger.info("Starting backup process for Incus VMs.")

    try:
        check_backup_dir(BACKUP_DIR)
    except RuntimeError as e:
        logger.error(e)
        return

    vm_names = get_vm_names()
    if not vm_names:
        logger.error("No VMs found or error retrieving instance list.")
        return

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    logger.debug(f"Timestamp for backup: {timestamp}")
    for vm in vm_names:
        logger.info(f"Processing VM: {vm}")
        try:
            prune_old_snapshots(vm, RETENTION_DAYS)
        except Exception as e:
            logger.error(f"Error during snapshot pruning for {vm}: {e}")

        snapshot_name = f"snapshot-{timestamp}"
        backup_file = f"{BACKUP_DIR.rstrip('/')}/{vm}-{timestamp}.tar.gz"
        try:
            snapshot_vm(vm, snapshot_name)
            export_vm(vm, backup_file)
        except Exception as e:
            logger.error(f"Skipping export for {vm} due to error: {e}")

if __name__ == "__main__":
    main()
