"""
Patched version of process_wikidata_dump.py with:
1. Safe key access for newer Wikidata dump formats
2. Support for .gz files (not just .bz2)
3. Optional multiprocessing support
"""
import bz2
import gzip
import json
import os
import argparse
from types import SimpleNamespace
from multiprocessing import Pool, cpu_count
from functools import partial

from tqdm.auto import tqdm


def extract_useful_info(entity):
    """Extract useful information from a Wikidata entity with safe key access."""
    qcode = entity.get('id', '')
    
    labels = entity.get('labels', {})
    if 'en' in labels:
        entity_en_label = labels['en'].get('value')
    else:
        entity_en_label = None
        
    descriptions = entity.get('descriptions', {})
    if 'en' in descriptions:
        entity_en_desc = descriptions['en'].get('value')
    else:
        entity_en_desc = None
        
    aliases = entity.get('aliases', {})
    if 'en' in aliases:
        entity_en_aliases = [alias.get('value', '') for alias in aliases['en']]
    else:
        entity_en_aliases = []
        
    sitelinks = entity.get('sitelinks', {})
    if 'enwiki' in sitelinks:
        enwiki_title = sitelinks['enwiki'].get('title')
    else:
        enwiki_title = None

    sitelinks_cnt = len(sitelinks)
    statements_cnt = 0
    triples = {}
    
    claims = entity.get('claims', {})
    for pcode, objs in claims.items():
        for obj in objs:
            statements_cnt += 1
            try:
                mainsnak = obj.get('mainsnak', {})
                datatype = mainsnak.get('datatype', '')
                snaktype = mainsnak.get('snaktype', '')
                
                # Skip if not a wikibase-item or missing datavalue
                if datatype != 'wikibase-item':
                    continue
                if snaktype == 'somevalue':
                    continue
                if 'datavalue' not in mainsnak:
                    continue
                    
                datavalue = mainsnak.get('datavalue', {})
                value = datavalue.get('value', {})
                target_id = value.get('id')
                
                if target_id:
                    if pcode not in triples:
                        triples[pcode] = []
                    triples[pcode].append(target_id)
            except Exception:
                # Skip malformed entries
                continue
                
    return {
        'qcode': qcode,
        'label': entity_en_label,
        'desc': entity_en_desc,
        'aliases': entity_en_aliases,
        'sitelinks_cnt': sitelinks_cnt,
        'enwiki': enwiki_title,
        'statements_cnt': statements_cnt,
        'triples': triples
    }


def process_line(line_bytes):
    """Process a single line (for multiprocessing). Returns list of (key, json_line) tuples."""
    if len(line_bytes) < 3:
        return []
    
    try:
        line = line_bytes.decode('utf-8').rstrip(',\n')
        entity = json.loads(line)
        entity_content = extract_useful_info(entity)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return []
    
    results = []
    qcode = entity_content['qcode']
    
    if 'P' in qcode:
        results.append(('properties', json.dumps({'qcode': qcode, 'values': entity_content})))
    
    if entity_content['sitelinks_cnt']:
        results.append(('sitelinks_cnt', json.dumps({'qcode': qcode, 'values': entity_content['sitelinks_cnt']})))
    
    if entity_content['statements_cnt']:
        results.append(('statements_cnt', json.dumps({'qcode': qcode, 'values': entity_content['statements_cnt']})))
    
    if entity_content['enwiki']:
        results.append(('enwiki', json.dumps({'qcode': qcode, 'values': entity_content['enwiki']})))
    
    if entity_content['desc']:
        results.append(('desc', json.dumps({'qcode': qcode, 'values': entity_content['desc']})))
    
    if entity_content['aliases']:
        results.append(('aliases', json.dumps({'qcode': qcode, 'values': entity_content['aliases']})))
    
    if entity_content['label']:
        results.append(('label', json.dumps({'qcode': qcode, 'values': entity_content['label']})))
    
    # Instance of relationships
    if 'P31' in entity_content['triples']:
        results.append(('instance_of_p31', json.dumps({'qcode': qcode, 'values': entity_content['triples']['P31']})))
        if 'Q5' in entity_content['triples']['P31'] or 'Q15632617' in entity_content['triples']['P31']:
            results.append(('humans', str(qcode)))
        if 'Q4167410' in entity_content['triples']['P31'] or 'Q22808320' in entity_content['triples']['P31']:
            results.append(('disambiguation', str(qcode)))
    
    if 'P131' in entity_content['triples']:
        results.append(('located_in_p131', json.dumps({'qcode': qcode, 'values': entity_content['triples']['P131']})))
    
    if 'P17' in entity_content['triples']:
        results.append(('country_p17', json.dumps({'qcode': qcode, 'values': entity_content['triples']['P17']})))
    
    if 'P641' in entity_content['triples']:
        results.append(('sport_p641', json.dumps({'qcode': qcode, 'values': entity_content['triples']['P641']})))
    
    if 'P106' in entity_content['triples']:
        results.append(('occupation_p106', json.dumps({'qcode': qcode, 'values': entity_content['triples']['P106']})))
    
    if 'P279' in entity_content['triples']:
        results.append(('subclass_p279', json.dumps({'qcode': qcode, 'values': entity_content['triples']['P279']})))
    
    results.append(('triples', json.dumps({'qcode': qcode, 'triples': entity_content['triples']})))
    
    return results


def build_wikidata_lookups(args_override=None):
    if args_override is None:
        parser = argparse.ArgumentParser(description='Build lookup dictionaries from Wikidata JSON dump.')
        parser.add_argument(
            "--dump_file_path",
            default='latest-all.json.bz2',
            type=str,
            help="file path to JSON Wikidata dump file (latest-all.json.bz2 or .json.gz)"
        )
        parser.add_argument(
            "--output_dir",
            type=str,
            default='output',
            help="Directory where the lookups will be stored"
        )
        parser.add_argument(
            "--overwrite_output_dir",
            action="store_true",
            help="Overwrite the content of the output directory"
        )
        parser.add_argument(
            "--test",
            action="store_true",
            help="mode for testing (only processes first 500 lines)"
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=1,
            help="Number of parallel workers (default=1, i.e., no multiprocessing)"
        )
        args = parser.parse_args()
    else:
        args = SimpleNamespace(**args_override)
        
    # Set default workers if not provided
    if not hasattr(args, 'workers'):
        args.workers = 1
        
    args.output_dir = args.output_dir.rstrip('/')
    number_lines_to_process = 500 if args.test else float('inf')
    
    if os.path.exists(args.output_dir) and os.listdir(args.output_dir) and not args.overwrite_output_dir:
        raise ValueError(f"Output directory ({args.output_dir}) already exists and is not empty. Use "
                         f"--overwrite_output_dir to overwrite.")
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    if os.path.exists(f'{args.output_dir}/sitelinks_cnt.json'):
        # the script has already been run so do not repeat the work.
        return

    filenames = [
        f'{args.output_dir}/sitelinks_cnt.json.part',
        f'{args.output_dir}/statements_cnt.json.part',
        f'{args.output_dir}/enwiki.json.part',
        f'{args.output_dir}/desc.json.part',
        f'{args.output_dir}/aliases.json.part',
        f'{args.output_dir}/qcode_to_label.json.part',
        f'{args.output_dir}/instance_of_p31.json.part',
        f'{args.output_dir}/country_p17.json.part',
        f'{args.output_dir}/sport_p641.json.part',
        f'{args.output_dir}/occupation_p106.json.part',
        f'{args.output_dir}/subclass_p279.json.part',
        f'{args.output_dir}/pcodes.json.part',
        f'{args.output_dir}/human_qcodes.json.part',
        f'{args.output_dir}/disambiguation_qcodes.txt.part',
        f'{args.output_dir}/triples.json.part',
        f'{args.output_dir}/located_in_p131.json.part',
    ]

    output_files = {
        'sitelinks_cnt': open(f'{args.output_dir}/sitelinks_cnt.json.part', 'w'),
        'statements_cnt': open(f'{args.output_dir}/statements_cnt.json.part', 'w'),
        'enwiki': open(f'{args.output_dir}/enwiki.json.part', 'w'),
        'desc': open(f'{args.output_dir}/desc.json.part', 'w'),
        'aliases': open(f'{args.output_dir}/aliases.json.part', 'w'),
        'label': open(f'{args.output_dir}/qcode_to_label.json.part', 'w'),
        'instance_of_p31': open(f'{args.output_dir}/instance_of_p31.json.part', 'w'),
        'country_p17': open(f'{args.output_dir}/country_p17.json.part', 'w'),
        'sport_p641': open(f'{args.output_dir}/sport_p641.json.part', 'w'),
        'occupation_p106': open(f'{args.output_dir}/occupation_p106.json.part', 'w'),
        'subclass_p279': open(f'{args.output_dir}/subclass_p279.json.part', 'w'),
        'properties': open(f'{args.output_dir}/pcodes.json.part', 'w'),
        'humans': open(f'{args.output_dir}/human_qcodes.json.part', 'w'),
        'disambiguation': open(f'{args.output_dir}/disambiguation_qcodes.txt.part', 'w'),
        'triples': open(f'{args.output_dir}/triples.json.part', 'w'),
        'located_in_p131': open(f'{args.output_dir}/located_in_p131.json.part', 'w'),
    }

    # Determine file opener based on extension
    dump_path = args.dump_file_path
    if dump_path.endswith('.bz2'):
        open_func = bz2.open
    elif dump_path.endswith('.gz'):
        open_func = gzip.open
    else:
        raise ValueError(f"Unsupported file format: {dump_path}. Expected .bz2 or .gz")

    i = 0
    use_multiprocessing = args.workers > 1
    
    print(f"Processing {dump_path} with {args.workers} worker(s)...")
    
    with open_func(dump_path, 'rb') as f:
        if use_multiprocessing:
            # Multiprocessing mode: process in batches
            batch_size = 10000
            pool = Pool(processes=args.workers)
            
            try:
                batch = []
                for line in tqdm(f, total=int(115e6), desc="Processing entities"):
                    i += 1
                    batch.append(line)
                    
                    if len(batch) >= batch_size:
                        # Process batch in parallel
                        results = pool.map(process_line, batch)
                        for line_results in results:
                            for key, json_line in line_results:
                                output_files[key].write(json_line + '\n')
                        batch = []
                    
                    if i > number_lines_to_process:
                        break
                
                # Process remaining batch
                if batch:
                    results = pool.map(process_line, batch)
                    for line_results in results:
                        for key, json_line in line_results:
                            output_files[key].write(json_line + '\n')
            finally:
                pool.close()
                pool.join()
        else:
            # Single-threaded mode (original behavior, more stable)
            for line in tqdm(f, total=int(115e6), desc="Processing entities"):
                i += 1
                line_results = process_line(line)
                for key, json_line in line_results:
                    output_files[key].write(json_line + '\n')
                
                if i > number_lines_to_process:
                    break

    for file in output_files.values():
        file.close()

    if not args.test:
        for filename in filenames:
            if os.path.exists(filename):
                os.rename(filename, filename.replace('.part', ''))
    
    print(f"Done! Processed {i:,} entities.")


if __name__ == '__main__':
    build_wikidata_lookups()
