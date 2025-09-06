import antismash

from dbimporter.common.record_data import RecordData

_FUNCTION_MAPPING = {}


def handle_genefunctions(data: RecordData):
    """Handle gene functions."""
    global _FUNCTION_MAPPING
    if not _FUNCTION_MAPPING:
        data.cursor.execute("SELECT functional_class_id, name FROM antismash.functional_classes")
        _FUNCTION_MAPPING = {name: func_id for func_id, name in data.cursor.fetchall()}

    genefunctions = data.module_results[antismash.detection.genefunctions.__name__].tool_results
    if not genefunctions:
        return
    

    all_smcog_results = genefunctions.smcogs
    all_extra_results = genefunctions.extras
    all_halogenase_results = genefunctions.halogenases
    all_mite_results = genefunctions.mite

    _insert_gene_function_mapping(data, _FUNCTION_MAPPING, all_smcog_results)
    _insert_gene_function_mapping(data, _FUNCTION_MAPPING, all_extra_results)
    _insert_gene_function_mapping(data, _FUNCTION_MAPPING, all_halogenase_results)
    _insert_gene_function_mapping(data, _FUNCTION_MAPPING, all_mite_results)


def _insert_gene_function_mapping(data, function_ids, results):
    if results and results.function_mapping:
        for cds_name, functions in results.subfunction_mapping.items():
            for function in functions:
                if function == "Halogenation":
                    function = "halogenase"
                elif function == "transport":
                    function = "transporter"
                elif function == "back translocase":
                    function = "back_translocase"
                function = function.lower()
                function_id = function_ids.get(function, None)
                if function_id is None:
                    print("unknown functional class:", function)
                    continue
                data.insert("""
                INSERT INTO antismash.gene_functions (cds_id, functional_class_id) VALUES
                ((SELECT cds_id FROM antismash.cdss WHERE locus_tag = %s), %s)
                ON CONFLICT DO NOTHING""", (cds_name, function_id))            