# mimic-log-extraction-tool

- requires python 3.8.10 (newer versions might be fine, though)
- using a python virtual environment seems like a good idea

## installation

Simply run the pip installation command to install the extraction tool:

```bash
pip install git+https://gitlab.hpi.de/finn.klessascheck/mimic-log-extraction-tool
```

Alternatively, clone this repo and execute

```bash
pip install -e .
```

For development and testing, all dev dependencies can be installed using

```bash
pip install -e .[dev]
```

If you're using `zsh`, escape the square brackets: `pip install -e .\[dev\]`
