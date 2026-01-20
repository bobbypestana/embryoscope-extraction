"""
Create a video from embryo image sequences stored in zip files.

This script extracts images from a zip file and creates a video with adjustable duration.
"""

import zipfile
import cv2
import numpy as np
from pathlib import Path
import tempfile
import os
from typing import Optional
import argparse
import re


def natural_sort_key(filename: str) -> tuple:
    """
    Create a sort key that handles numeric parts correctly.
    Extracts the RUN number and converts it to an integer for proper sorting.
    """
    # Extract the RUN number from filenames like "RUN123"
    match = re.search(r'RUN(\d+)', filename)
    if match:
        return (int(match.group(1)),)
    return (0,)


def create_video_from_zip(
    zip_path: str,
    output_path: str,
    video_duration_seconds: float = 30.0,
    fps: Optional[int] = None,
    focal_plane: Optional[str] = None
) -> None:
    """
    Create a video from images in a zip file.
    
    Args:
        zip_path: Path to the zip file containing images
        output_path: Path where the output video will be saved
        video_duration_seconds: Total duration of the video in seconds (default: 30.0)
        fps: Frames per second. If None, will be calculated based on duration
        focal_plane: Specific folder/focal plane to use (e.g., 'F0', 'F15'). If None, uses all images.
    """
    print(f"Processing: {zip_path}")
    
    # Open the zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Get list of image files
        all_image_files = [f for f in zip_ref.namelist() if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
        
        # Filter by focal plane if specified
        if focal_plane:
            image_files = [f for f in all_image_files if f.startswith(f"{focal_plane}/")]
            if not image_files:
                print(f"No images found in folder '{focal_plane}'")
                print(f"Available folders: {set([f.split('/')[0] for f in all_image_files if '/' in f])}")
                return
            print(f"Using focal plane: {focal_plane}")
        else:
            image_files = all_image_files
        
        # Sort them using natural sorting
        image_files = sorted(image_files, key=natural_sort_key)
        
        if not image_files:
            print("No image files found in the zip!")
            return
        
        num_images = len(image_files)
        print(f"Found {num_images} images")
        
        # Calculate FPS based on desired duration
        if fps is None:
            fps = max(1, int(num_images / video_duration_seconds))
        
        actual_duration = num_images / fps
        print(f"Video settings: {fps} FPS, {actual_duration:.2f} seconds duration")
        
        # Create temporary directory to extract images
        with tempfile.TemporaryDirectory() as temp_dir:
            print("Extracting images...")
            
            # Extract first image to get dimensions
            first_image_data = zip_ref.read(image_files[0])
            first_image = cv2.imdecode(np.frombuffer(first_image_data, np.uint8), cv2.IMREAD_COLOR)
            height, width, _ = first_image.shape
            
            print(f"Image dimensions: {width}x{height}")
            
            # Initialize video writer
            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            video_writer = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
            
            # Process all images
            for i, image_file in enumerate(image_files):
                if i % 50 == 0:
                    print(f"Processing image {i+1}/{num_images}...")
                
                # Read image from zip
                image_data = zip_ref.read(image_file)
                image = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)
                
                # Write frame to video
                video_writer.write(image)
            
            # Release video writer
            video_writer.release()
    
    print(f"Video created successfully: {output_path}")
    print(f"Final video: {num_images} frames at {fps} FPS = {actual_duration:.2f} seconds")


def main():
    parser = argparse.ArgumentParser(description='Create a video from embryo image sequences in zip files')
    parser.add_argument('zip_file', type=str, help='Path to the zip file containing images')
    parser.add_argument('--output', '-o', type=str, help='Output video path (default: same name as zip with .mp4 extension)')
    parser.add_argument('--duration', '-d', type=float, default=30.0, help='Video duration in seconds (default: 30.0)')
    parser.add_argument('--fps', type=int, help='Frames per second (if not specified, calculated from duration)')
    parser.add_argument('--focal-plane', '-f', type=str, help='Specific focal plane folder to use (e.g., F0, F15)')
    
    args = parser.parse_args()
    
    # Set default output path if not provided
    if args.output is None:
        zip_path = Path(args.zip_file)
        focal_suffix = f"_{args.focal_plane}" if args.focal_plane else ""
        output_path = str(zip_path.parent / f"{zip_path.stem}{focal_suffix}.mp4")
    else:
        output_path = args.output
    
    # Create the video
    create_video_from_zip(
        zip_path=args.zip_file,
        output_path=output_path,
        video_duration_seconds=args.duration,
        fps=args.fps,
        focal_plane=args.focal_plane
    )


if __name__ == "__main__":
    main()
