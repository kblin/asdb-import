# License: GNU Affero General Public License v3 or later
# A copy of GNU AGPL v3 should have been included in this software package in LICENSE.txt.

""" A package to help prepare records to be ready for importing """

import antismash
from antismash.common.secmet.locations import CompoundLocation, FeatureLocation

DEFAULT_AS_OPTIONS = antismash.config.build_config(["--minimal"], modules=antismash.main.get_all_modules())
DEFAULT_AS_OPTIONS.all_enabled_modules = []


def prepare_record(record, areas, module_results):
    cc = antismash.common.secmet.features.CandidateCluster
    record.strip_antismash_annotations()
    for raw_region in areas:
        protoclusters = {}
        for index, proto in raw_region["protoclusters"].items():
            if proto["start"] > proto["end"]:
                full = CompoundLocation([
                    FeatureLocation(proto["start"], len(record)),
                    FeatureLocation(0, proto["end"])
                ])
            else:
                full = FeatureLocation(proto["start"], proto["end"])

            if proto["core_start"] > proto["core_end"]:
                core = CompoundLocation([
                    FeatureLocation(proto["core_start"], len(record)),
                    FeatureLocation(0, proto["core_end"])
                ])
            else:
                core = FeatureLocation(proto["core_start"], proto["core_end"])
            protoclusters[int(index)] = antismash.common.secmet.Protocluster(core, full, proto["tool"], proto["product"], 0, 0, 0, "")
        candidates = []
        for cand in raw_region["candidates"]:
            cand_pcs = [protoclusters[i] for i in cand["protoclusters"]]
            candidates.append(cc(kind=cc.kinds.from_string(cand["kind"]), protoclusters=cand_pcs, circular_wrap_point=len(record)))
        for p_feature in protoclusters.values():
            record.add_protocluster(p_feature)
        for c_feature in candidates:
            record.add_candidate_cluster(c_feature)
        # TODO subregions
        record.add_region(antismash.common.secmet.Region(candidate_clusters=candidates))

    def regen(raw, module):
        assert raw
        regenerated = module.regenerate_previous_results(raw, record, DEFAULT_AS_OPTIONS)
        assert regenerated is not None, "%s results failed to generate for %s" % (module.__name__, record.id)
        regenerated.add_to_record(record)
        return regenerated

    for module in antismash.main.get_detection_modules():
        if module.__name__ in module_results:
            module_results[module.__name__] = regen(module_results[module.__name__], module)

    for module in antismash.main.get_analysis_modules():
        if module.__name__ in module_results:
            module_results[module.__name__] = regen(module_results[module.__name__], module)

    for val in module_results.values():
        assert not isinstance(val, dict)
