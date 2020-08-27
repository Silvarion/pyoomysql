import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mysqllib-silvarion", # Replace with your own username
    version="0.0.1",
    author="Jesus Sanchez",
    author_email="silvarion@gmail.com",
    description="A library to interact with MySQL databases in an object-oriented way",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Silvarion/mysqllib",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
