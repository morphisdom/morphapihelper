import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="morphapiwrapper",
    version="1.0",
    author="Morphisdom",
    author_email="info@morphisdom.com",
    description="Wrapper functions for creating APIs in Morphisdom",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/morphisdom/morphapiwrapper",
    project_urls={
        "Bug Tracker": "https://github.com/morphisdom/morphapiwrapper/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
	keywords = ['API constructor', 'Flask API', 'Google cloud storage base','microservices'],
	install_requires=[ 'flask','requests', 'google_cloud_storage','google-auth']
)