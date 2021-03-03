import setuptools

setuptools.setup(
    name="retrace_core",
    version="0.0.1",
    author="Selene Blok",
    author_email="s.blok@vwt.digital",
    description="The process mining core package",
    url="https://github.com/vwt-digital-solutions/dan-process-mining-core",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
