import antismash

from dbimporter.common.record_data import RecordData


_TERPENE_MAPPING = {}


def handle_terpenes(data: RecordData):
    """Handle terpene annotations."""
    global _TERPENE_MAPPING
    if not _TERPENE_MAPPING:
        data.cursor.execute("SELECT * FROM antismash.terpene_domains")
        _TERPENE_MAPPING = {name: terpene_id for terpene_id, name in data.cursor.fetchall()}

    terpenes = data.module_results[antismash.modules.terpene.__name__].cluster_predictions
    if not terpenes:
        return
    
    for _, protocluster in terpenes.items():
        for locus_tag, cds_results in protocluster.cds_predictions.items():
            for cds_result in cds_results:
                terpene = cds_result.domain_type
                terpene_id = _TERPENE_MAPPING.get(terpene, None)
                if terpene_id is None:
                    print("unknown terpene domain:", terpene)
                    continue
                data.insert("""
                INSERT INTO antismash.terpene_hits (cds_id, terpene_domain_id, location) VALUES
                ((SELECT cds_id FROM antismash.cdss WHERE locus_tag = %s), %s, %s)
                ON CONFLICT DO NOTHING""", (locus_tag, terpene_id, f"{cds_result.start}-{cds_result.end}"))