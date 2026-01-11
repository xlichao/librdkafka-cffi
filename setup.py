from setuptools import setup, find_packages
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO)

setup(
    name="librdkafka_cffi",
    version='0.0.2',
    description='CFFI-based Python wrapper for librdkafka',
    author='Your Name',
    author_email='your.email@example.com',
    packages=find_packages(exclude=["wrapper_generator", "wrapper_generator.*"]),
    include_package_data=True,
    setup_requires=['cffi>=1.0.0'],
    cffi_modules=["wrapper_generator/_build.py:ffibuilder"],
    install_requires=['cffi>=1.0.0'],
    zip_safe=False,
    license='MIT',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries",
    ],
)