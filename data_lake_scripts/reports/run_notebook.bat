@echo off
echo Starting Jupyter notebook for unmatched embryo analysis...
echo.
echo Make sure you have the try_request conda environment activated.
echo.
echo The notebook will open in your default browser.
echo You can then:
echo 1. Run all cells to see the analysis
echo 2. Use the interactive functions to investigate specific cases
echo 3. Modify queries to explore different aspects
echo.
pause

jupyter notebook analyze_unmatched_embryos_interactive.ipynb 