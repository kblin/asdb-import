#!/usr/bin/env python3
"""Import NCBI assembly ID to strain collection identifier mappings into the database."""

from argparse import ArgumentParser, FileType
import logging

import psycopg2
import psycopg2.extensions


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
DB_CONNECTION = "postgresql://postgres:secret@localhost:5432/antismash"
SUPPORTED_TABLES = {
    "nbc": "nbc_collection",
    "npdc": "npdc_collection",
    "dsmz": "dsmz_collection",
}


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=DB_CONNECTION, help="Database connection string")
    parser.add_argument("mappings", type=FileType("r"), nargs="+",
                        metavar="MAPPINGS",
                        help="File(s) containing NCBI assembly ID to strain collection identifier mappings")
    parser.add_argument("--debug", "-d", action="store_true",
                        help="Enable debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)


    connection = psycopg2.connect(args.db)
    connection.autocommit = False

    with connection.cursor() as cursor:
        for mapping_file in args.mappings:
            table_name = None
            for key, value in SUPPORTED_TABLES.items():
                if mapping_file.name.startswith(key):
                    table_name = value
                    break
            if not table_name:
                logging.error("Unsupported mapping file: %s", mapping_file.name)
                continue

            logging.info("Processing mappings from %s into %s", mapping_file.name, table_name)
            _insert_mappings(cursor, table_name, mapping_file)
    connection.commit()
    connection.close()


def _insert_mappings(cursor, table_name, mappings):
    """Insert NCBI assembly ID to strain collection identifier mappings into the database."""
    for i, line in enumerate(mappings):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            assembly_id, identifier = line.split("\t")
            logging.debug("%s -> %s", assembly_id, identifier)
        except ValueError:
            logging.warning("Skipping malformed line %d: %s", i, line)
            continue
        cursor.execute(
            "SELECT genome_id FROM antismash.genomes WHERE assembly_id = %s",
            (assembly_id,)
        )
        genome_id = cursor.fetchone()
        if not genome_id:
            logging.warning("No genome found for assembly ID %s", assembly_id)
            continue
        cursor.execute(
            f"INSERT INTO antismash.{table_name} (genome_id, identifier) "
            "VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (genome_id, identifier)
        )

    
if __name__ == "__main__":
    main()
