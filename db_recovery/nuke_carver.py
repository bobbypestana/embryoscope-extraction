import os
import argparse
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs): return iterable

def extract_jpeg(data, start, end):
    """
    Extracts JPEG and attempts to strip the 4-byte SQLite overflow pointers
    that occur at every 4096-byte page boundary, assuming contiguous allocation.
    """
    page_size = 4096
    first_boundary = ((start // page_size) + 1) * page_size
    
    if end < first_boundary:
        return data[start:end]
        
    clean_data = bytearray()
    clean_data.extend(data[start:first_boundary])
    
    curr = first_boundary
    while curr < end:
        # Skip 4 byte SQLite pointer
        curr += 4
        next_boundary = curr + (page_size - 4)
        chunk_end = min(end, next_boundary)
        clean_data.extend(data[curr:chunk_end])
        curr = chunk_end
        
    return clean_data

def nuke_carve(db_path, output_dir):
    pdb_file_name = os.path.splitext(os.path.basename(db_path))[0]
    print(f"Starting THE NUKE OPTION (Raw Signature Carver) on {pdb_file_name}...")
    
    # We will output to a special lost_and_found directory
    target_dir = os.path.join(output_dir, pdb_file_name, "lost_and_found")
    os.makedirs(target_dir, exist_ok=True)
    
    print("Reading file into memory...")
    with open(db_path, 'rb') as f:
        data = f.read()
        
    print(f"File loaded. Size: {len(data) / (1024*1024):.2f} MB")
    
    sig = b'\xff\xd8\xff\xe0'
    end_sig = b'\xff\xd9'
    
    offset = 0
    count = 0
    
    pbar = tqdm(total=len(data), desc="Sweeping Disk Bytes")
    
    while True:
        start = data.find(sig, offset)
        if start == -1: 
            pbar.update(len(data) - offset)
            break
            
        pbar.update(start - offset)
        
        # Found start, look for end
        end = data.find(end_sig, start)
        if end == -1 or (end - start) > 150000: # JPEGs shouldn't be larger than 150KB
            offset = start + 4
            continue
            
        # Extract and clean
        jpeg_data = extract_jpeg(data, start, end + 2)
        
        count += 1
        out_path = os.path.join(target_dir, f"nuke_recovered_{count:06d}.jpg")
        with open(out_path, "wb") as img_f:
            img_f.write(jpeg_data)
            
        offset = end + 2
        pbar.update(offset - start)

    pbar.close()
    print(f"\nNuke Carving Complete!")
    print(f"Raw Images Found & Extracted: {count}")
    print(f"Check the '{target_dir}' folder.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", help="Path to the PDB file")
    parser.add_argument("--output_dir", default=r"g:\My Drive\projetos_individuais\Huntington\db_recovery\extracted_images", help="Output directory")
    args = parser.parse_args()
    
    nuke_carve(args.db_path, args.output_dir)
