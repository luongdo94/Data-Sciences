from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="erp_importer",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A Python application for importing and processing data from ERP systems",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/erp_importer",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Office/Business",
    ],
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.3.0",
        "pyodbc>=4.0.30",
        "python-dotenv>=0.19.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.2.5",
            "pytest-cov>=2.12.1",
            "black>=21.7b0",
            "flake8>=3.9.2",
            "mypy>=0.910",
            "isort>=5.9.3",
        ],
    },
    entry_points={
        "console_scripts": [
            "erp-import=erp_importer.main:main",
        ],
    },
)
