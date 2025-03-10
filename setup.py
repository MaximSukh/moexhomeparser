from setuptools import setup, find_packages

setup(
    name='MOEXHOMEPARSER',
    version='0.1.0',
    description='A module for parsing MOEX financial data',
    author='Sukhanov_Maxim',
    author_email='mssukukhanov@gmail.com',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pandas',
        'numpy',
        'requests',
        'certifi',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
