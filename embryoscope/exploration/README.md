# Exploration Files

This folder contains scripts and notebooks for exploring and analyzing embryoscope data.

## Files

- `explore_embryoscope_data.ipynb` - Jupyter notebook for interactive data exploration
- `quick_explore.py` - Command-line script for quick database overview
- `query_interface.py` - Python interface for querying embryoscope data

## Usage

From the main embryoscope directory:

```bash
# Quick database overview
python exploration/quick_explore.py

# Interactive exploration (requires Jupyter)
jupyter notebook exploration/explore_embryoscope_data.ipynb

# Use query interface
python exploration/query_interface.py
```

## Note

All exploration files have been updated to work from their new location. Database paths have been adjusted to point to the correct location (`../../database/`). 