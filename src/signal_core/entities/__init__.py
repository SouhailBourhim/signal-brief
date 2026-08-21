"""Entity resolution. SPEC §7.2.

`dictionary` builds and loads the alias dictionary; `resolve` is the single decision seam,
shared by the Spark job and by `evals/score.py` the way `dedup.decide` is.
"""
