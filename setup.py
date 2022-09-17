from setuptools import setup, find_packages
import pathlib

setup(
    name="potafloes",
    version="0.0.1",
    description="Python dataflow library",
    long_description=pathlib.Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    package_data={"potafloes": ["py.typed"]},
    packages=find_packages(where="src"),
    author="Guillaume Everarts de Velp",
    author_email="edvgui@gmail.com",
    license="MIT",
    url="https://github.com/edvgui/potafloes",
    python_requires=">=3.10",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.10',
        'Topic :: Software Development :: Quality Assurance',
    ],
    keywords="dataflow asyncio",
)
