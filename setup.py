import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyoomysql", # Replace with your own username
    version="0.2.1",
    author="Jesus Alejandro Sanchez Davila",
    author_email="jsanchez.consultant@gmail.com",
    description="Python Object-Orinted MySQL interface",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Silvarion/pyoomysql",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'mysql-connector-python'
    ]
)
