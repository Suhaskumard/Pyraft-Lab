"""The laboratory: scenario runner, metrics, plots, stress and chaos campaigns. (Phase 5)

``manifest.py``/``replay.py`` (Phase 7): every run persists what it needs to be run
again, and ``replay_run`` proves determinism by doing exactly that and comparing traces.

``stress.py``/``chaos.py`` (Phase 8): many-trial campaigns over the same runner - a
bounded catalog of generated fault combinations, and an open-ended randomized fault
sequence checked against one safety invariant, respectively. A failing trial persists
via ``replay.persist_run`` and is a `pyraft-lab replay <run_id>` away from reproducing.
"""
