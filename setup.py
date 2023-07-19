import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='agol_pandas',                     # name of the package
    version='0.0.1',                        # release version
    author='GeoCodable',                    # org/author
    description=\
        '''
        agol_pandas
        Pandas helper functions for ArcGIS Online hosted tables.   
          - Direct read, write, and create to and from pandas from hosted tables. 
          - Included options include append, overwrite, upsert, insert, update. 
          - All modes include the option to chunk data while writing data to hosted tables
        ''',
    long_description=long_description,      # long description read from the the readme file
    long_description_content_type='text/markdown',
    packages=setuptools.find_packages(),    # list of all python modules to be installed
    classifiers=[                           # information to filter the project on PyPi website
                        'Programming Language :: Python :: 3',
                        'License :: OSI Approved :: MIT License',
                        'Operating System :: OS Independent',
                        'Natural Language :: English',
                        'Programming Language :: Python :: 3.9',
                        ],                                      
    python_requires='>=3.9',                # minimum version requirement of the package
    py_modules=['agol_pandas'],             # name of the python package
    package_dir={'':'src'},                 # directory of the source code of the package
    install_requires=[                      # package dependencies
                        'arcgis==1.9.1',
                        'pandas>=1.1',
                        'tqdm>=4.64.1,
                    ]
    )
