#!/usr/bin/env python3
"""Exécuter seulement les exercices 1.1 et 2.1, dans le conteneur Spark."""
from pathlib import Path

import nbformat
from nbclient import NotebookClient

path = Path('/workspace/notebooks/J2_Spark_Delta_Silver.ipynb')
notebook = nbformat.read(path, as_version=4)
partial = nbformat.v4.new_notebook(cells=notebook.cells[:6], metadata=notebook.metadata)
partial.cells.append(nbformat.v4.new_code_cell('spark.stop()'))
NotebookClient(partial, timeout=600, kernel_name='python3',
               resources={'metadata': {'path': '/workspace'}}).execute()
notebook.cells[:6] = partial.cells[:6]
nbformat.write(notebook, path)
print('Exercices 1.1 et 2.1 exécutés ; résultats enregistrés dans le notebook.')
