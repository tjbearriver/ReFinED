"""
Fast parallel Wikidata dump processor using:
1. orjson for 10x faster JSON parsing
2. Multiprocessing with proper queue-based producer/consumer pattern
3. pbzip2 for parallel decompression
"""
import bz2
import gzip
import os
import argparse
import subprocess
import sys
from types import SimpleNamespace
from multiprocessing import Process, Queue, cpu_count, Value
from ctypes import c_long
import io
import signal

try:
    import orjson as json
    def json_loads(s):
        return json.loads(s)
    def json_dumps(obj):
        return json.dumps(obj).decode('utf-8')
except ImportError:
    import json
    def json_loads(s):
        return json.loads(s)
    def json_dumps(obj):
        return json.dumps(obj)

from tqdm.auto import tqdm


def extract_useful_info(entity):
    """Extract useful information from a Wikidata entity with safe key access."""
    qcode = entity.get('id', '')
    
    labels = entity.get('labels', {})
    entity_en_label = labels.get('en', {}).get('value') if 'en' in labels else None
        
    descriptions = entity.get('descriptions', {})
    entity_en_desc = descriptions.get('en', {}).get('value') if 'en' in descriptions else None
        
    aliases = entity.get('aliases', {})
    entity_en_aliases = [alias.get('value', '') for alias in aliases.get('en', [])] if 'en' in aliases else []
        
    sitelinks = entity.get('sitelinks', {})
    enwiki_title = sitelinks.get('enwiki', {}).get('title') if 'enwiki' in sitelinks else None

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
                
                if datatype != 'wikibase-item' or snaktype == 'somevalue' or 'datavalue' not in mainsnak:
                    continue
                    
                target_id = mainsnak.get('datavalue', {}).get('value', {}).get('id')
                if target_id:
                    if pcode not in triples:
                        triples[pcode] = []
                    triples[pcode].append(target_id)
            except Exception:
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


def process_entity(ec, output_buffers):
    """Process a single entity and append to output buffers."""
    qcode = ec['qcode']
    
    if 'P' in qcode:
        output_buffers['properties'].append(json_dumps({'qcode': qcode, 'values': ec}))
    
    if ec['sitelinks_cnt']:
        output_buffers['sitelinks_cnt'].append(json_dumps({'qcode': qcode, 'values': ec['sitelinks_cnt']}))
    
    if ec['statements_cnt']:
        output_buffers['statements_cnt'].append(json_dumps({'qcode': qcode, 'values': ec['statements_cnt']}))
    
    if ec['enwiki']:
        output_buffers['enwiki'].append(json_dumps({'qcode': qcode, 'values': ec['enwiki']}))
    
    if ec['desc']:
        output_buffers['desc'].append(json_dumps({'qcode': qcode, 'values': ec['desc']}))
    
    if ec['aliases']:
        output_buffers['aliases'].append(json_dumps({'qcode': qcode, 'values': ec['aliases']}))
    
    if ec['label']:
        output_buffers['label'].append(json_dumps({'qcode': qcode, 'values': ec['label']}))
    
    if 'P31' in ec['triples']:
        output_buffers['instance_of_p31'].append(json_dumps({'qcode': qcode, 'values': ec['triples']['P31']}))
        if 'Q5' in ec['triples']['P31'] or 'Q15632617' in ec['triples']['P31']:
            output_buffers['humans'].append(str(qcode))
        if 'Q4167410' in ec['triples']['P31'] or 'Q22808320' in ec['triples']['P31']:
            output_buffers['disambiguation'].append(str(qcode))
    
    if 'P131' in ec['triples']:
        output_buffers['located_in_p131'].append(json_dumps({'qcode': qcode, 'values': ec['triples']['P131']}))
    
    if 'P17' in ec['triples']:
        output_buffers['country_p17'].append(json_dumps({'qcode': qcode, 'values': ec['triples']['P17']}))
    
    if 'P641' in ec['triples']:
        output_buffers['sport_p641'].append(json_dumps({'qcode': qcode, 'values': ec['triples']['P641']}))
    
    if 'P106' in ec['triples']:
        output_buffers['occupation_p106'].append(json_dumps({'qcode': qcode, 'values': ec['triples']['P106']}))
    
    if 'P279' in ec['triples']:
        output_buffers['subclass_p279'].append(json_dumps({'qcode': qcode, 'values': ec['triples']['P279']}))
    
    output_buffers['triples'].append(json_dumps({'qcode': qcode, 'triples': ec['triples']}))


def process_batch(lines):
    """Process a batch of lines. Returns dict of key -> list of json lines."""
    results = {
        'sitelinks_cnt': [], 'statements_cnt': [], 'enwiki': [], 'desc': [],
        'aliases': [], 'label': [], 'instance_of_p31': [], 'country_p17': [],
        'sport_p641': [], 'occupation_p106': [], 'subclass_p279': [],
        'properties': [], 'humans': [], 'disambiguation': [], 'triples': [],
        'located_in_p131': []
    }
    
    for line_str in lines:
        if len(line_str) < 3:
            continue
        try:
            line = line_str.rstrip(',\n')
            entity = json_loads(line)
            ec = extract_useful_info(entity)
            process_entity(ec, results)
        except:
            continue
    
    return results


def worker_process(work_queue, result_queue, worker_id):
    """Worker that processes batches from work queue."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    while True:
        try:
            batch = work_queue.get(timeout=30)
            if batch is None:  # Poison pill
                break
            result = process_batch(batch)
            result_queue.put(result)
        except:
            break


def get_default_workers():
    return max(1, cpu_count() - 4)


def build_wikidata_lookups(args_override=None):
    default_workers = get_default_workers()
    
    if args_override is None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--dump_file_path", default='latest-all.json.bz2', type=str)
        parser.add_argument("--output_dir", type=str, default='output')
        parser.add_argument("--overwrite_output_dir", action="store_true")
        parser.add_argument("--test", action="store_true")
        parser.add_argument("--workers", type=int, default=default_workers)
        args = parser.parse_args()
    else:
        args = SimpleNamespace(**args_override)
        
    if not hasattr(args, 'workers') or args.workers is None:
        args.workers = default_workers
        
    args.output_dir = args.output_dir.rstrip('/')
    number_lines_to_process = 500 if args.test else float('inf')
    
    if os.path.exists(args.output_dir) and os.listdir(args.output_dir) and not args.overwrite_output_dir:
        raise ValueError(f"Output directory ({args.output_dir}) already exists.")
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    if os.path.exists(f'{args.output_dir}/sitelinks_cnt.json'):
        return

    filenames = {
        'sitelinks_cnt': f'{args.output_dir}/sitelinks_cnt.json.part',
        'statements_cnt': f'{args.output_dir}/statements_cnt.json.part',
        'enwiki': f'{args.output_dir}/enwiki.json.part',
        'desc': f'{args.output_dir}/desc.json.part',
        'aliases': f'{args.output_dir}/aliases.json.part',
        'label': f'{args.output_dir}/qcode_to_label.json.part',
        'instance_of_p31': f'{args.output_dir}/instance_of_p31.json.part',
        'country_p17': f'{args.output_dir}/country_p17.json.part',
        'sport_p641': f'{args.output_dir}/sport_p641.json.part',
        'occupation_p106': f'{args.output_dir}/occupation_p106.json.part',
        'subclass_p279': f'{args.output_dir}/subclass_p279.json.part',
        'properties': f'{args.output_dir}/pcodes.json.part',
        'humans': f'{args.output_dir}/human_qcodes.json.part',
        'disambiguation': f'{args.output_dir}/disambiguation_qcodes.txt.part',
        'triples': f'{args.output_dir}/triples.json.part',
        'located_in_p131': f'{args.output_dir}/located_in_p131.json.part',
    }

    output_files = {key: open(path, 'w', buffering=8*1024*1024) for key, path in filenames.items()}

    dump_path = args.dump_file_path
    decompress_proc = None
    
    # Use parallel decompression
    if dump_path.endswith('.bz2'):
        pbzip2_check = subprocess.run(['which', 'pbzip2'], capture_output=True)
        if pbzip2_check.returncode == 0:
            decompress_workers = 64
            print(f"Using pbzip2 with {decompress_workers} threads for decompression...")
            decompress_cmd = ['pbzip2', '-dc', f'-p{decompress_workers}', dump_path]
            decompress_proc = subprocess.Popen(decompress_cmd, stdout=subprocess.PIPE, bufsize=256*1024*1024)
            input_stream = io.TextIOWrapper(decompress_proc.stdout, encoding='utf-8', errors='replace', newline='\n')
        else:
            print("pbzip2 not found, using Python bz2...")
            input_stream = bz2.open(dump_path, 'rt', encoding='utf-8')
    elif dump_path.endswith('.gz'):
        input_stream = gzip.open(dump_path, 'rt', encoding='utf-8')
    else:
        raise ValueError(f"Unsupported format: {dump_path}")

    # Use fewer workers to avoid queue contention, but enough to keep CPU busy
    num_workers = min(args.workers, 32)
    print(f"Using {num_workers} workers for JSON parsing (orjson)...")
    
    work_queue = Queue(maxsize=num_workers * 4)
    result_queue = Queue()
    
    workers = []
    for i in range(num_workers):
        p = Process(target=worker_process, args=(work_queue, result_queue, i))
        p.start()
        workers.append(p)
    
    batch_size = 5000
    current_batch = []
    line_count = 0
    batches_sent = 0
    batches_received = 0
    
    pbar = tqdm(total=int(115e6), desc="Processing", unit=" entities", smoothing=0.05)
    
    def collect_results():
        nonlocal batches_received
        while not result_queue.empty():
            try:
                result = result_queue.get_nowait()
                batches_received += 1
                for key, lines in result.items():
                    if lines:
                        output_files[key].write('\n'.join(lines) + '\n')
            except:
                break
    
    try:
        for line in input_stream:
            current_batch.append(line)
            line_count += 1
            
            if len(current_batch) >= batch_size:
                # Non-blocking check for results
                collect_results()
                
                # Send batch (will block if queue full - backpressure)
                work_queue.put(current_batch)
                batches_sent += 1
                current_batch = []
                pbar.update(batch_size)
                pbar.set_postfix(sent=batches_sent, done=batches_received)
            
            if line_count > number_lines_to_process:
                break
                
    except KeyboardInterrupt:
        print("\nInterrupted!")
    finally:
        # Send remaining batch
        if current_batch:
            work_queue.put(current_batch)
            batches_sent += 1
        
        # Send poison pills
        for _ in workers:
            work_queue.put(None)
        
        # Wait for workers and collect remaining results
        print(f"\nWaiting for {batches_sent - batches_received} remaining batches...")
        for p in workers:
            p.join(timeout=60)
        
        # Collect any remaining results
        while batches_received < batches_sent:
            try:
                result = result_queue.get(timeout=5)
                batches_received += 1
                for key, lines in result.items():
                    if lines:
                        output_files[key].write('\n'.join(lines) + '\n')
            except:
                break
        
        pbar.close()
        
        if decompress_proc:
            decompress_proc.terminate()
            decompress_proc.wait()

        for f in output_files.values():
            f.close()

        if not args.test:
            for key, path in filenames.items():
                if os.path.exists(path):
                    os.rename(path, path.replace('.part', ''))
    
    print(f"Done! Processed {line_count:,} entities.")


if __name__ == '__main__':
    build_wikidata_lookups()
