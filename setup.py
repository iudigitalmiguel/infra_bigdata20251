from setuptools import setup, find_packages

setup(
    name="bigdata_actividad1",
    version="0.0.1",
    author="Miguel Angel Leal Beltran",
    author_email="miguel.leal@est.iudigital.edu.co",
    description="proyecto integrador infraestructura de BigData",
    py_modules=["actividad_1"],
    install_requires=[
        "pandas",
        "openpyxl",
        "requests"
    ]
    
    
)