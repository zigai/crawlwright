import pathlib

from setuptools import find_packages, setup

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()
REQUIREMENTS = (HERE / "requirements.txt").read_text()

setup(
    name="crawlwright",
    version="0.0.1",
    description="Web crawler powered by Playwright",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Žiga Ivanšek",
    author_email="ziga.ivansek@gmail.com",
    url="https://github.com/zigai/crawlwright",
    license="MIT",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    packages=find_packages(),
    install_requires=REQUIREMENTS,
)
