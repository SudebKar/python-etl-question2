import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="HistoricalSalesAnalysis_SudebKar",
    version="0.0.1",
    author="sudeb-kar",
    author_email="sudebkr@gmail.com",
    description="Historical data pull of Sales, Region and Customer tables",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)