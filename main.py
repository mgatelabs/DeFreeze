import re
import shutil
import subprocess
from glob import glob
import os
import argparse

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


def cut_gaps(file_path, output_file, force: bool = False):
    print(f"Analysing: {file_path}")
    freezes = detect_freezes(file_path)
    silences = detect_silences(file_path)

    # Merge only overlapping video & audio gaps of at least 3 seconds
    gaps = merge_intervals(freezes, silences, min_duration=1.5)

    if not gaps:
        if force:
            print(f"No significant gaps detected in {file_path}. Encoding the original file.")

            cmd = [
                "ffmpeg", "-i", file_path,
                 "-profile:v", "high", "-level", "4.2", "-crf", "28", "-movflags",
                "+faststart", "-c:a", "aac", "-b:a", "128k", "-preset", 'slower', output_file
            ]

            print(f"Running FFmpeg command: {' '.join(cmd)}")
            subprocess.run(cmd, check=True)

            return True
        else:
            print(f"No significant gaps detected in {file_path}. Copying the original file.")
            # subprocess.run(["copy", file_path, output_file])  # Use shutil.copy for cross-platform
            shutil.copy(file_path, output_file)
            return True

    inputs = []
    filter_inputs = []
    last_end = 0
    segment_index = 0

    for start, end in gaps:
        if last_end > 0 and abs(last_end - start) <= 0.001:
            continue
        # Add segment from last_end to start of gap
        inputs.extend(["-ss", str(last_end), "-to", str(start), "-i", file_path])
        filter_inputs.append(f"[{segment_index}:v:0][{segment_index}:a:0]")
        last_end = end
        segment_index += 1

    # Add final segment after last gap
    inputs.extend(["-ss", str(last_end), "-i", file_path])
    filter_inputs.append(f"[{segment_index}:v:0][{segment_index}:a:0]")

    # Construct the concat filter
    filter_complex = f"{''.join(filter_inputs)}concat=n={segment_index + 1}:v=1:a=1[outv][outa]"

    cmd = [
        "ffmpeg", *inputs,
        "-filter_complex", filter_complex,
        "-map", "[outv]", "-map", "[outa]", "-profile:v", "high","-level", "4.2", "-crf", "28", "-movflags", "+faststart", "-c:a", "aac", "-b:a", "128k", "-preset", 'slower', output_file
    ]

    print(f"Running FFmpeg command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    return True

def main():
    parser = argparse.ArgumentParser(description="Process data with optional encoding.")
    #parser.add_argument("data", type=str, help="The data to process")
    parser.add_argument("--forceEncode", action="store_true", help="Enable encoding")

    args = parser.parse_args()

    input_folder = "in"
    output_folder = "out"
    os.makedirs(output_folder, exist_ok=True)

    for file in glob(os.path.join(input_folder, "*.mp4")):
        filename = os.path.basename(file)
        output_file = os.path.join(output_folder, filename)
        if cut_gaps(file, output_file, args.forceEncode):
                os.unlink(file)
        print(f"Processed: {filename}")

if __name__ == "__main__":
    main()