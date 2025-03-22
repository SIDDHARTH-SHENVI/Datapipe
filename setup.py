from setuptools import setup, find_packages

setup(
    name="datapipe",
    version="1.0.0",
    description="A CLI tool for distributed data processing with Redis and PostgreSQL",
    author="Siddharth",
    author_email="siddharthshenvi49@gmail.com",
    packages=find_packages(),
    install_requires=["redis", "python-dotenv", "psycopg2-binary", "pyyaml", "pandas", "rich"],
    entry_points={"console_scripts": ["datapipe = datapipe.cli:main"]},
    python_requires=">=3.6",
    keywords=["data-processing", "task-queue", "cli", "distributed"],
    license="MIT"
)