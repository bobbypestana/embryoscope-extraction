import zipfile
import os

zip_path = r"g:\My Drive\projetos_individuais\Huntington\embryoscope\02_embryo_images_extraction\export_images\887615_D2025.06.28_S04307_I3166_P-2\images.zip"

print(f"Inspecting: {zip_path}")

try:
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        print(f"Total files: {len(file_list)}")
        
        # Check for focal plane folders
        folders = set()
        for name in file_list:
            parts = name.split('/')
            if len(parts) > 1:
                folders.add(parts[0])
        
        print(f"Found folders: {sorted(list(folders))}")
        
        # Print first 10 files
        print("Sample files:")
        for name in file_list[:10]:
            print(f"  - {name}")
            
except Exception as e:
    print(f"Error: {e}")
