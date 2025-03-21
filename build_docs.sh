
#!/bin/bash

# Install Sphinx if not already installed
pip install -r requirements.txt

# Create docs directory if it doesn't exist
mkdir -p docs
cd docs

# Initialize Sphinx documentation
sphinx-quickstart --quiet --project="Music Streaming ETL" --author="Your Name" --sep --ext-autodoc --ext-viewcode

# Build the documentation
make html

echo "Documentation built! Open docs/build/html/index.html in your browser"