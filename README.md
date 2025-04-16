# Incus VM Backup Automation

A Python script to automate snapshot creation, backup exports, and retention management for Incus virtual machines.

## Features

- 🕒 Automated snapshot management with configurable retention policy
- 📦 VM export to compressed tarballs
- 📈 Backup speed and duration metrics tracking
- 🗑️ Intelligent pruning of old snapshots
- 📝 Detailed logging with Debug mode
- ⏰ Timestamped backup files

## Prerequisites

- Python 3.6+
- Incus 6.0+
- Properly configured Incus environment
- Write access to backup directory
- `incus` CLI available in PATH

## Usage
```git clone https://github.com/nickf1227/incus-backup.git && cd incus-backup && python3 incus_backup.py```

