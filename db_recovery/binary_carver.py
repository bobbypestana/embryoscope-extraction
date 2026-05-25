import os
import struct
import argparse
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs): return iterable

def read_varint(data, offset):
    v = 0
    for i in range(8):
        if offset >= len(data): break
        b = data[offset]
        offset += 1
        v = (v << 7) | (b & 0x7f)
        if (b & 0x80) == 0:
            return v, offset
    if offset < len(data):
        b = data[offset]
        offset += 1
        v = (v << 8) | b
    return v, offset

def get_serial_length_and_value(serial_type, data, offset):
    if serial_type == 0: return 0, None, offset
    if serial_type == 1: 
        if offset + 1 > len(data): return 1, None, offset+1
        val = struct.unpack_from(">b", data, offset)[0]
        return 1, val, offset+1
    if serial_type == 2:
        if offset + 2 > len(data): return 2, None, offset+2
        val = struct.unpack_from(">h", data, offset)[0]
        return 2, val, offset+2
    if serial_type == 3:
        if offset + 3 > len(data): return 3, None, offset+3
        b = data[offset:offset+3]
        val = int.from_bytes(b, byteorder='big', signed=True)
        return 3, val, offset+3
    if serial_type == 4:
        if offset + 4 > len(data): return 4, None, offset+4
        val = struct.unpack_from(">i", data, offset)[0]
        return 4, val, offset+4
    if serial_type == 5:
        if offset + 6 > len(data): return 6, None, offset+6
        b = data[offset:offset+6]
        val = int.from_bytes(b, byteorder='big', signed=True)
        return 6, val, offset+6
    if serial_type == 6:
        if offset + 8 > len(data): return 8, None, offset+8
        val = struct.unpack_from(">q", data, offset)[0]
        return 8, val, offset+8
    if serial_type == 7:
        if offset + 8 > len(data): return 8, None, offset+8
        val = struct.unpack_from(">d", data, offset)[0]
        return 8, val, offset+8
    if serial_type == 8: return 0, 0, offset
    if serial_type == 9: return 0, 1, offset
    if serial_type >= 12 and serial_type % 2 == 0:
        length = (serial_type - 12) // 2
        return length, data[offset:offset+length], offset+length
    if serial_type >= 13 and serial_type % 2 == 1:
        length = (serial_type - 13) // 2
        try:
            val = data[offset:offset+length].decode('utf-8', errors='replace')
        except:
            val = None
        return length, val, offset+length
    return 0, None, offset

def get_payload(f, offset_in_file, U, P):
    M = ((P - 12) * 64 // 255) - 23
    m = ((P - 12) * 32 // 255) - 23
    
    if U <= P - 35:
        X = U
    else:
        K = M + ((U - M) % (P - 4))
        if K <= M:
            X = K
        else:
            X = m

    f.seek(offset_in_file)
    payload = f.read(X)
    
    if U > X:
        next_page = struct.unpack(">I", f.read(4))[0]
        bytes_left = U - X
        while next_page != 0 and bytes_left > 0:
            f.seek((next_page - 1) * P)
            next_page = struct.unpack(">I", f.read(4))[0]
            to_read = min(bytes_left, P - 4)
            payload += f.read(to_read)
            bytes_left -= to_read
            
    return payload

def carve_database(db_path, output_base_dir):
    pdb_file_name = os.path.splitext(os.path.basename(db_path))[0]
    print(f"Starting Binary Carving on {pdb_file_name}...")
    
    if not os.path.exists(output_base_dir):
        os.makedirs(output_base_dir)

    with open(db_path, "rb") as f:
        header = f.read(100)
        if len(header) < 100 or header[:16] != b'SQLite format 3\x00':
            print("Not a valid SQLite database.")
            return
            
        P = struct.unpack_from(">H", header, 16)[0]
        if P == 1: P = 65536
        print(f"Page Size: {P} bytes")
        
        f.seek(0, 2)
        file_size = f.tell()
        total_pages = file_size // P
        print(f"Total Pages to scan: {total_pages}")
        
        extracted_count = 0
        skipped_existing = 0
        
        # Metrics trackers
        corrupted_cells = 0
        valid_cells = 0
        total_leaf_pages = 0

        for page_idx in tqdm(range(total_pages), desc="Scanning Pages"):
            page_offset = page_idx * P
            f.seek(page_offset)
            page_header = f.read(8)
            if not page_header: break
            
            if page_header[0] == 0x0D:
                total_leaf_pages += 1
                try:
                    num_cells = struct.unpack_from(">H", page_header, 3)[0]
                    cell_pointers_offset = page_offset + 8
                    f.seek(cell_pointers_offset)
                    pointers = f.read(2 * num_cells)
                    
                    for i in range(num_cells):
                        if i*2 + 2 > len(pointers): 
                            corrupted_cells += 1
                            continue
                            
                        cell_offset = struct.unpack_from(">H", pointers, i*2)[0]
                        if cell_offset == 0: continue
                        
                        cell_abs_offset = page_offset + cell_offset
                        f.seek(cell_abs_offset)
                        cell_header_data = f.read(20)
                        if not cell_header_data: 
                            corrupted_cells += 1
                            continue
                        
                        try:
                            U, off = read_varint(cell_header_data, 0)
                            rowid, off = read_varint(cell_header_data, off)
                            
                            if U < 1000: 
                                # Not an image, but it's a valid cell structurally
                                valid_cells += 1
                                continue 
                            
                            payload = get_payload(f, cell_abs_offset + off, U, P)
                            if not payload: 
                                corrupted_cells += 1
                                continue
                            
                            header_size, off2 = read_varint(payload, 0)
                            if header_size > len(payload): 
                                corrupted_cells += 1
                                continue 
                            
                            serial_types = []
                            curr = off2
                            while curr < header_size:
                                st, curr = read_varint(payload, curr)
                                serial_types.append(st)
                                
                            if len(serial_types) >= 5:
                                st_img = serial_types[4]
                                if st_img >= 12 and st_img % 2 == 0:
                                    data_offset = header_size
                                    
                                    _, well, data_offset = get_serial_length_and_value(serial_types[0], payload, data_offset)
                                    _, run, data_offset = get_serial_length_and_value(serial_types[1], payload, data_offset)
                                    _, focal, data_offset = get_serial_length_and_value(serial_types[2], payload, data_offset)
                                    _, time_val, data_offset = get_serial_length_and_value(serial_types[3], payload, data_offset)
                                    _, image_blob, data_offset = get_serial_length_and_value(serial_types[4], payload, data_offset)
                                    
                                    if isinstance(well, int) and isinstance(run, int) and image_blob:
                                        dir_path = os.path.join(output_base_dir, pdb_file_name, f"{pdb_file_name}-{well}", f"F{focal}")
                                        if not os.path.exists(dir_path):
                                            os.makedirs(dir_path)
                                            
                                        filename = f"{pdb_file_name}-{well}-RUN{int(run):04d}.jpg"
                                        filepath = os.path.join(dir_path, filename)
                                        
                                        if os.path.exists(filepath):
                                            skipped_existing += 1
                                        else:
                                            with open(filepath, "wb") as img_f:
                                                img_f.write(image_blob)
                                            extracted_count += 1
                                            
                            valid_cells += 1
                        except Exception as e:
                            corrupted_cells += 1
                except Exception:
                    corrupted_cells += 1

        print(f"\n" + "="*50)
        print(f"CORRUPTION & SALVAGE REPORT")
        print(f"="*50)
        
        # Calculate Index of Corruption
        total_cells = valid_cells + corrupted_cells
        corruption_index = (corrupted_cells / total_cells * 100) if total_cells > 0 else 0
        print(f"Index of Corruption: {corruption_index:.2f}%")
        print(f"  - Valid Cells Found: {valid_cells}")
        print(f"  - Corrupted/Unreadable Cells: {corrupted_cells}")
        
        # Calculate Salvage Rate
        estimated_total_images = file_size / 35000  # rough 35KB estimate
        total_salvaged = extracted_count + skipped_existing
        salvage_rate = (total_salvaged / estimated_total_images * 100)
        print(f"\nRestoration Success Rate: {salvage_rate:.2f}% (vs absolute file size)")
        print(f"  - Images Salvaged (This run): {extracted_count}")
        print(f"  - Images Skipped (Already existed): {skipped_existing}")
        print(f"  - Total Intact Images: {total_salvaged}")
        print(f"  - Theoretical Max Images: ~{int(estimated_total_images)}")
        print(f"="*50)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", help="Path to the PDB file")
    parser.add_argument("--output_dir", default=r"g:\My Drive\projetos_individuais\Huntington\db_recovery\extracted_images", help="Output directory")
    args = parser.parse_args()
    
    carve_database(args.db_path, args.output_dir)
