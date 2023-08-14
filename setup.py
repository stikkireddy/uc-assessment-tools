from setuptools import setup, find_packages

setup(
    name='databricks-uc-assessments',
    author='Dipankar Kushari, Gary Diana, Sri Tikkireddy',
    author_email='dipankar.kushari@databricks.com, gary.diana@databricks.com, sri.tikkireddy@databricks.com',
    description='A package for a ui to do misc stuff in databricks',
    packages=find_packages(exclude=['notebooks']),
    setup_requires=['setuptools_scm'],
    install_requires=[
        "databricks-sdk>=0.4.0, <1.0.0",
        "solara>=1.19.0, <2.0.0",
        "gitpython",
        "pyre2"
    ],
    license_files=('LICENSE',),
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='Databricks Clusters',
)
