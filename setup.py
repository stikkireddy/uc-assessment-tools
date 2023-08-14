from setuptools import setup, find_packages

reqs = open("requirements.txt").read().split("\n")

setup(
    name='databricks-uc-assessments',
    author='Dipankar Kushari, Gary Diana, Sri Tikkireddy',
    author_email='dipankar.kushari@databricks.com, gary.diana@databricks.com, sri.tikkireddy@databricks.com',
    description='A package for a ui to do misc stuff in databricks',
    packages=find_packages(exclude=['notebooks']),
    setup_requires=['setuptools_scm'],
    install_requires=reqs,
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
