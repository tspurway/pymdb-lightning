from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

setup(
    cmdclass = {'build_ext': build_ext},
    ext_modules = [Extension("mdb", ["db.pyx", ],
        libraries=["lmdb"],
        library_dirs=["./lib"],
        include_dirs=["./lib"],
        runtime_library_dirs=["/usr/local/lib"])]
)
