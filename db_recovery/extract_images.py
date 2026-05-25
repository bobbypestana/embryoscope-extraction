import sqlite3
import os
import argparse
try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm is not installed
    def tqdm(iterable, **kwargs):
        return iterable

def save_image(output_base_dir, pdb_file_name, well, focal, run, image_blob):
    dir_path = os.path.join(output_base_dir, pdb_file_name, f"{pdb_file_name}-{well}", f"F{focal}")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        
    filename = f"{pdb_file_name}-{well}-RUN{int(run):04d}.jpg"
    filepath = os.path.join(dir_path, filename)
    
    if os.path.exists(filepath):
        return False  # Skipped
        
    if image_blob is not None:
        with open(filepath, "wb") as f:
            f.write(image_blob)
    return True  # Extracted

def extract_images_fast(conn, pdb_file_name, output_base_dir):
    cur = conn.cursor()
    print("Attempting Fast Extraction...")
    
    try:
        cur.execute("SELECT COUNT(*) FROM IMAGES")
        total_images = cur.fetchone()[0]
        print(f"Total images found: {total_images}")
        
        cur.execute("SELECT Well, Run, Focal, Time, Image FROM IMAGES")
        
        extracted_count = 0
        skipped_existing = 0
        
        # Use tqdm if available, otherwise just iterate
        for row in tqdm(cur, total=total_images, desc="Extracting (Fast)"):
            well, run, focal, time, image_blob = row
            if save_image(output_base_dir, pdb_file_name, well, focal, run, image_blob):
                extracted_count += 1
            else:
                skipped_existing += 1
                
        print(f"\nFast Extraction complete! Extracted: {extracted_count}, Skipped: {skipped_existing}")
        return True
        
    except sqlite3.DatabaseError as e:
        print(f"\nFast extraction failed due to database corruption: {e}")
        return False

def extract_images_jumping(conn, pdb_file_name, output_base_dir):
    cur = conn.cursor()
    print("Falling back to RowID Jumping extraction...")

    max_rowid = 0
    try:
        cur.execute("SELECT MAX(rowid) FROM IMAGES")
        max_rowid = cur.fetchone()[0] or 0
        print(f"Estimated total RowIDs based on MAX(rowid): {max_rowid}")
    except sqlite3.DatabaseError:
        print("Could not estimate total rows (corrupt index). Percentages will not be exact.")

    current_id = 0
    extracted_count = 0
    corrupted_count = 0
    skipped_existing = 0
    max_consecutive_fails = 50000
    fails = 0

    while True:
        try:
            cur.execute("SELECT rowid, Well, Run, Focal, Time, Image FROM IMAGES WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (current_id,))
            row = cur.fetchone()
            
            if not row:
                print("\nReached the end of the database.")
                break
                
            current_id, well, run, focal, time, image_blob = row
            fails = 0  
            
            if save_image(output_base_dir, pdb_file_name, well, focal, run, image_blob):
                extracted_count += 1
            else:
                skipped_existing += 1
                
            if (extracted_count + skipped_existing) % 100 == 0:
                pct_str = ""
                if max_rowid > 0:
                    ext_pct = ((extracted_count + skipped_existing) / max_rowid) * 100
                    cor_pct = (corrupted_count / max_rowid) * 100
                    pct_str = f" | Extracted: {ext_pct:.2f}% | Corrupt: {cor_pct:.4f}%"
                print(f"Extracted: {extracted_count} (Skipped: {skipped_existing}) | Corrupt Skips: {corrupted_count} | RowID: {current_id}{pct_str}", end="\r", flush=True)

        except sqlite3.DatabaseError:
            current_id += 1
            corrupted_count += 1
            fails += 1
            
            if fails % 1000 == 0:
                pct_str = ""
                if max_rowid > 0:
                    cor_pct = (corrupted_count / max_rowid) * 100
                    pct_str = f" | Total Corrupt: {cor_pct:.4f}%"
                print(f"Skipping corrupt block... Fails: {fails} | RowID: {current_id}{pct_str}", end="\r", flush=True)
            
            if fails > max_consecutive_fails:
                print(f"\nExceeded max consecutive failures ({max_consecutive_fails}). Assuming end of intact data.")
                break

    print(f"\n\nRowID Jumping Extraction complete!")
    print(f"Total newly extracted images: {extracted_count}")
    print(f"Total existing images skipped: {skipped_existing}")
    print(f"Total corrupted blocks skipped: {corrupted_count}")
    if max_rowid > 0:
        final_corrupt_pct = (corrupted_count / max_rowid) * 100
        print(f"Percentage of corrupted blocks: {final_corrupt_pct:.4f}%")

def extract_images(db_path, output_base_dir):
    if not os.path.exists(output_base_dir):
        os.makedirs(output_base_dir)

    print(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)
    pdb_file_name = os.path.splitext(os.path.basename(db_path))[0]
    
    # 1. Try normal fast extraction
    success = extract_images_fast(conn, pdb_file_name, output_base_dir)
    
    # 2. Fallback to RowID jumping if corrupt
    if not success:
        extract_images_jumping(conn, pdb_file_name, output_base_dir)
        
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract images from PDB SQLite database with corruption fallback")
    parser.add_argument("db_path", help="Path to the PDB file")
    parser.add_argument("--output_dir", default=r"g:\My Drive\projetos_individuais\Huntington\db_recovery\extracted_images", help="Output directory")
    args = parser.parse_args()
    
    extract_images(args.db_path, args.output_dir)
