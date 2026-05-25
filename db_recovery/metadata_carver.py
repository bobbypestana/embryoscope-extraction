import os
import struct
import argparse
import csv
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

def is_str(st): return st >= 13 and st % 2 == 1
def is_int(st): return st <= 6 or st in (8, 9)
def is_float(st): return st == 7
def is_numeric(st): return is_int(st) or is_float(st)

def carve_metadata(db_path, output_dir):
    pdb_file_name = os.path.splitext(os.path.basename(db_path))[0]
    print(f"Starting Metadata Carving on {pdb_file_name}...")
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    general_rows = []
    blastomere_rows = []

    with open(db_path, "rb") as f:
        header = f.read(100)
        if len(header) < 100 or header[:16] != b'SQLite format 3\x00':
            print("Not a valid SQLite database.")
            return
            
        P = struct.unpack_from(">H", header, 16)[0]
        if P == 1: P = 65536
        
        f.seek(0, 2)
        file_size = f.tell()
        total_pages = file_size // P
        
        for page_idx in tqdm(range(total_pages), desc="Scanning Pages"):
            page_offset = page_idx * P
            f.seek(page_offset)
            page_header = f.read(8)
            if not page_header: break
            
            # 0x0D is Table B-Tree Leaf Page
            if page_header[0] == 0x0D:
                num_cells = struct.unpack_from(">H", page_header, 3)[0]
                cell_pointers_offset = page_offset + 8
                
                f.seek(cell_pointers_offset)
                pointers = f.read(2 * num_cells)
                
                for i in range(num_cells):
                    if i*2 + 2 > len(pointers): continue
                    cell_offset = struct.unpack_from(">H", pointers, i*2)[0]
                    if cell_offset == 0: continue
                    
                    cell_abs_offset = page_offset + cell_offset
                    f.seek(cell_abs_offset)
                    # For metadata, cells are small. 
                    # 400 bytes is more than enough for a full metadata row.
                    cell_data = f.read(400)
                    if not cell_data: continue
                    
                    try:
                        U, off = read_varint(cell_data, 0) # payload size
                        rowid, off = read_varint(cell_data, off) # rowid
                        
                        # Metadata payloads are small
                        if U > 300 or off >= len(cell_data): continue
                        
                        payload = cell_data[off:off+U]
                        header_size, off2 = read_varint(payload, 0)
                        if header_size > len(payload): continue
                        
                        serial_types = []
                        curr = off2
                        while curr < header_size:
                            st, curr = read_varint(payload, curr)
                            serial_types.append(st)
                            
                        # Fingerprint 1: GENERAL -> [String, String, Numeric, String]
                        if len(serial_types) == 4 and is_str(serial_types[0]) and is_str(serial_types[1]) and is_numeric(serial_types[2]) and is_str(serial_types[3]):
                            data_offset = header_size
                            _, col1, data_offset = get_serial_length_and_value(serial_types[0], payload, data_offset)
                            _, col2, data_offset = get_serial_length_and_value(serial_types[1], payload, data_offset)
                            _, col3, data_offset = get_serial_length_and_value(serial_types[2], payload, data_offset)
                            _, col4, data_offset = get_serial_length_and_value(serial_types[3], payload, data_offset)
                            general_rows.append([col1, col2, col3, col4])

                        # Fingerprint 2: BlastomereData/WellData -> [Int, String, Numeric, Numeric]
                        elif len(serial_types) == 4 and is_int(serial_types[0]) and is_str(serial_types[1]) and is_numeric(serial_types[2]) and is_numeric(serial_types[3]):
                            data_offset = header_size
                            _, col1, data_offset = get_serial_length_and_value(serial_types[0], payload, data_offset)
                            _, col2, data_offset = get_serial_length_and_value(serial_types[1], payload, data_offset)
                            _, col3, data_offset = get_serial_length_and_value(serial_types[2], payload, data_offset)
                            _, col4, data_offset = get_serial_length_and_value(serial_types[3], payload, data_offset)
                            blastomere_rows.append([col1, col2, col3, col4])
                            
                    except Exception as e:
                        pass

    # Save outputs
    print(f"\nCarving Complete!")
    print(f"Recovered {len(general_rows)} rows for GENERAL table.")
    print(f"Recovered {len(blastomere_rows)} rows for BlastomereData/WellData tables.")
    
    target_dir = os.path.join(output_dir, pdb_file_name)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    
    if general_rows:
        gen_path = os.path.join(target_dir, f"{pdb_file_name}_GENERAL_recovered.csv")
        with open(gen_path, "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Type', 'Par', 'Time', 'Val'])
            writer.writerows(general_rows)
            
    if blastomere_rows:
        blst_path = os.path.join(target_dir, f"{pdb_file_name}_Well_Blastomere_recovered.csv")
        with open(blst_path, "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Well', 'Parameter', 'Time', 'Value'])
            writer.writerows(blastomere_rows)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", help="Path to the PDB file")
    parser.add_argument("--output_dir", default=r"g:\My Drive\projetos_individuais\Huntington\db_recovery\extracted_images", help="Output directory")
    args = parser.parse_args()
    
    carve_metadata(args.db_path, args.output_dir)
