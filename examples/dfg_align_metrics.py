import pm4py
from pm4py.algo.conformance.alignments.dfg.variants import classic as dfg_alignments


def execute_script():
    log = pm4py.read_xes("../tests/input_data/receipt.xes")
    dfg, sa, ea = pm4py.discover_dfg(pm4py.filter_variants_top_k(log, 5))

    conformance_dfg, activities_conformance = dfg_alignments.project_alignments_on_dfg(log, dfg, sa, ea)
    print(conformance_dfg)
    print(activities_conformance)


if __name__ == "__main__":
    execute_script()
