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
RETENTION_DAYS = 7                         # Days to retain snapshots
BACKUP_RETENTION_DAYS = 14                 # Days to retain backup files
BACKUP_DIR = "/mnt/ice/incus-vms/"         # Directory for exported backups
LOG_FILE = "/var/log/incus-backup-nickf.log" # Log file path
STATEFUL_SNAPSHOTS = False                  # Create snapshots with running state, including process memory state, TCP connections, etc
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
    """Prunes snapshots for a given VM that are older than retention_days."""
    logger.info(f"Starting snapshot pruning for {vm_name}")
    
    try:
        # Get snapshot list in JSON format
        result = run_command(["incus", "snapshot", "list", vm_name, "--format", "json"],
                            description=f"listing snapshots for {vm_name}")
        snapshots = json.loads(result.stdout)
    except Exception as e:
        logger.error(f"Failed to get snapshots for {vm_name}: {e}")
        return

    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    logger.debug(f"Snapshot cutoff time (UTC): {cutoff.isoformat()}")

    for snap in snapshots:
        try:
            snapshot_name = snap["name"]
            created_str = snap["created_at"]
            
            # Parse ISO 8601 timestamp with fractional seconds
            created_at = parse_iso_datetime(created_str)

            if created_at < cutoff:
                logger.info(f"Deleting snapshot {snapshot_name} (created {created_at.isoformat()} UTC)")
                run_command(["incus", "snapshot", "delete", vm_name, snapshot_name],
                           description=f"delete snapshot {snapshot_name}")
            else:
                logger.debug(f"Keeping recent snapshot {snapshot_name} (created {created_at.isoformat()} UTC)")

        except KeyError as e:
            logger.error(f"Missing field in snapshot data: {e}")
        except ValueError as e:
            logger.error(f"Failed to parse timestamp {created_str}: {e}")
        except Exception as e:
            logger.error(f"Error processing snapshot: {e}")

def snapshot_vm(vm_name, snapshot_name):
    """
    Creates a snapshot of the specified VM using 'incus snapshot create'.
    Uses --stateful flag when STATEFUL_SNAPSHOTS is True to preserve running state.
    """
    logger.info(f"Taking {'stateful ' if STATEFUL_SNAPSHOTS else ''}snapshot for VM '{vm_name}'")
    
    cmd = ["incus", "snapshot", "create"]
    if STATEFUL_SNAPSHOTS:
        cmd.append("--stateful")
    
    cmd += [vm_name, snapshot_name]

    try:
        run_command(cmd, description=f"creating {'stateful ' if STATEFUL_SNAPSHOTS else ''}snapshot")
        logger.debug(f"Snapshot {snapshot_name} created successfully")
    except subprocess.CalledProcessError as e:
        if STATEFUL_SNAPSHOTS:
            logger.warning("Stateful snapshot failed, attempting stateless...")
            try:
                run_command(["incus", "snapshot", "create", vm_name, snapshot_name],
                           description="fallback stateless snapshot")
            except Exception as fallback_e:
                logger.error(f"Failed both stateful and stateless snapshots: {fallback_e}")
                raise
        else:
            logger.error(f"Snapshot creation failed: {e}")
            raise
    except Exception as e:
        logger.error(f"Unexpected error during snapshot: {e}")
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
                # Extract timestamp from filename format: vmname-YYYYMMDDHHMMSS.tar.gz
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
    except RuntimeError as e:
        logger.error(e)
        return

    vm_names = get_vm_names()
    if not vm_names:
        logger.error("No VMs found or error retrieving instance list.")
        return

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    logger.debug(f"Timestamp for backup: {timestamp}")
    
    # Process all VMs first
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
    # Prune old backups after processing all VMs
    prune_old_backups()

if __name__ == "__main__":
    main()
