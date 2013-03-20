from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

setup(
    cmdclass = {'build_ext': build_ext},
    ext_modules = [Extension("mdb", ["db.pyx", ],
        libraries=["lmdb"],
        library_dirs=["/usr/local/lib"],
        include_dirs=["/usr/local/include"],
        runtime_library_dirs=["/usr/local/lib"])]
)
