import argparse
import os
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import zipfile

def detect_freezes(file_path):
    cmd = [
        "ffmpeg", "-i", file_path,
        "-vf", "freezedetect=n=-60dB:d=0.5",
        "-map", "0:v:0", "-f", "null", "-"
    ]
    result = subprocess.run(cmd, stderr=subprocess.PIPE, text=True)
    timestamps = re.findall(r"freeze_start: (\d+\.\d+)|freeze_end: (\d+\.\d+)", result.stderr)
    freeze_ranges = []
    start = None
    for start_time, end_time in timestamps:
        if start_time:
            start = float(start_time)
        if end_time and start is not None:
            freeze_ranges.append((start, float(end_time)))
            start = None
    return freeze_ranges


def detect_silences(file_path):
    cmd = [
        "ffmpeg", "-i", file_path,
        "-af", "silencedetect=noise=-30dB:d=0.5",
        "-f", "null", "-"
    ]

    result = subprocess.run(cmd, stderr=subprocess.PIPE, text=True)
    timestamps = re.findall(r"silence_start: (\d+\.\d+)|silence_end: (\d+\.\d+)", result.stderr)
    silence_ranges = []
    start = None

    for start_time, end_time in timestamps:
        if start_time:
            start = float(start_time)
        if end_time and start is not None:
            silence_ranges.append((start, float(end_time)))
            start = None
    return silence_ranges


def merge_intervals(video_gaps, audio_gaps, min_duration=3.0):
    merged = []

    # Find overlapping intervals between video and audio gaps
    i, j = 0, 0
    while i < len(video_gaps) and j < len(audio_gaps):
        v_start, v_end = video_gaps[i]
        a_start, a_end = audio_gaps[j]

        # Find the overlapping region
        overlap_start = max(v_start, a_start)
        overlap_end = min(v_end, a_end)

        # If overlap duration is at least min_duration, keep it
        if overlap_end > overlap_start and (overlap_end - overlap_start) >= min_duration:
            merged.append((overlap_start, overlap_end))

        # Move to the next interval in whichever list finishes first
        if v_end < a_end:
            i += 1
        else:
            j += 1

    return merged


def cut_gaps_with_analysis(file_path, output_file, freezes, silences, force: bool = False, verbose: bool = False):
    gaps = merge_intervals(freezes, silences, min_duration=1.5)

    if not gaps:
        if force:
            print(f"No significant gaps in {file_path}. Encoding the original file.")
            cmd = [
                "ffmpeg", "-i", file_path,
                "-profile:v", "high", "-level", "4.2", "-crf", "30", "-movflags",
                "+faststart", "-c:a", "aac", "-b:a", "128k", "-preset", 'slower', output_file
            ]
            subprocess.run(cmd, check=True,
                           stdout=None if verbose else subprocess.DEVNULL,
                           stderr=None if verbose else subprocess.DEVNULL)
        else:
            print(f"No significant gaps in {file_path}. Copying original file.")
            shutil.copy(file_path, output_file)
        return True

    inputs = []
    filter_inputs = []
    last_end = 0
    segment_index = 0

    for start, end in gaps:
        if last_end > 0 and abs(last_end - start) <= 0.001:
            continue
        inputs.extend(["-ss", str(last_end), "-to", str(start), "-i", file_path])
        filter_inputs.append(f"[{segment_index}:v:0][{segment_index}:a:0]")
        last_end = end
        segment_index += 1

    inputs.extend(["-ss", str(last_end), "-i", file_path])
    filter_inputs.append(f"[{segment_index}:v:0][{segment_index}:a:0]")

    filter_complex = f"{''.join(filter_inputs)}concat=n={segment_index + 1}:v=1:a=1[outv][outa]"

    cmd = [
        "ffmpeg", *inputs,
        "-filter_complex", filter_complex,
        "-map", "[outv]", "-map", "[outa]", "-profile:v", "high", "-level", "4.2", "-crf", "30", "-movflags",
        "+faststart", "-c:a", "aac", "-b:a", "128k", "-preset", 'slower', output_file
    ]
    subprocess.run(cmd, check=True,
                   stdout=None if verbose else subprocess.DEVNULL,
                   stderr=None if verbose else subprocess.DEVNULL)

    return True


def analyze_file(input_file):
    print(f"Analyzing: {input_file}")
    freezes = detect_freezes(input_file)
    silences = detect_silences(input_file)
    return input_file, freezes, silences


def encode_wrapper(job):
    input_file, freezes, silences, output_file, force, verbose = job
    print(f"Encoding: {input_file}")
    try:
        if cut_gaps_with_analysis(input_file, output_file, freezes, silences, force, verbose):
            os.unlink(input_file)
        print(f"Finished: {input_file}")
    except Exception as e:
        print(f"Error encoding {input_file}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Multi-threaded gap cutter.")
    parser.add_argument("--forceEncode", action="store_true", help="Force encode even if no gaps")
    parser.add_argument("--threads", type=int, default=3, help="Number of encoding threads")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose subprocess output")
    args = parser.parse_args()

    input_folder = "in"
    output_folder = "out"

    files_to_process = []

    # Step 1: Preprocess ZIP files
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.lower().endswith(".zip"):
                zip_path = os.path.join(root, file)
                print(f"Extracting: {zip_path}")
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(root)
                    os.remove(zip_path)
                    print(f"Deleted zip: {zip_path}")
                except zipfile.BadZipFile:
                    print(f"Skipping bad zip file: {zip_path}")

    # Step 2: Find MP4 files to process
    for root, _, files in os.walk(input_folder):
        for file in files:
            if not file.endswith(".mp4"):
                continue
            input_file = os.path.join(root, file)
            relative_path = os.path.relpath(root, input_folder)
            output_dir = os.path.join(output_folder, relative_path)
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, file)
            if os.path.exists(output_file):
                print(f"Skipping (already exists): {output_file}")
                continue
            files_to_process.append((input_file, output_file))

    # Step 1: Serial or Parallel Analysis
    print(f"Starting analysis for {len(files_to_process)} files...")
    analysis_results = []
    for input_file, output_file in files_to_process:
        analysis_results.append((*analyze_file(input_file), output_file, args.forceEncode, args.verbose))

    # Step 2: Multithreaded Encoding
    print(f"Encoding using {args.threads} threads...")
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [executor.submit(encode_wrapper, job) for job in analysis_results]
        for f in as_completed(futures):
            f.result()  # Will raise exception if any

    print("All done.")


if __name__ == "__main__":
    main()
